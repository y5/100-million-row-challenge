<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function implode;
use function ini_set;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const SEEK_CUR;
use const WNOHANG;

final class Parser
{
    private const int READ_CHUNK = 262_144;
    private const int PROBE_SIZE = 2_097_152;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);

        // Map every valid "YY-MM-DD" to a sequential integer
        $dpm = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        $dateIds = [];
        $dateLabels = [];
        $dateCount = 0;

        for ($year = 2020; $year <= 2026; $year++) {
            $yy = $year - 2000;
            for ($m = 0; $m < 12; $m++) {
                $days = $dpm[$m] + ($m === 1 && $year % 4 === 0 ? 1 : 0);
                $prefix = sprintf('%d-%02d-', $yy, $m + 1);
                for ($d = 1; $d <= $days; $d++) {
                    $key = $prefix . sprintf('%02d', $d);
                    $dateIds[$key] = $dateCount;
                    $dateLabels[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        // Probe head of file to discover slugs in first-seen order
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $probe = fread($handle, $fileSize > self::PROBE_SIZE ? self::PROBE_SIZE : $fileSize);
        fclose($handle);

        $slugBases = [];
        $slugLabels = [];
        $slugCount = 0;

        // Trim to last complete line, then split and extract slug from each
        $lines = explode("\n", substr($probe, 0, strrpos($probe, "\n")));
        unset($probe);

        foreach ($lines as $line) {
            $comma = strpos($line, ',');
            if ($comma === false) continue;
            $slug = substr($line, 25, $comma - 25);
            if (!isset($slugBases[$slug])) {
                $slugBases[$slug] = $slugCount * $dateCount;
                $slugLabels[$slugCount] = $slug;
                $slugCount++;
            }
        }
        unset($lines);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($slugBases[$slug])) {
                $slugBases[$slug] = $slugCount * $dateCount;
                $slugLabels[$slugCount] = $slug;
                $slugCount++;
            }
        }

        $workers = 8;

        // With Appple Silicon stuff we have faster perf cores and slow efficiency cores
        // The perf cores should get bigger chunks so they finish at roughly the same time
        // Mac fills perf cores first, so the first forks get bigger chunks
        $perfCores = (int) (@\shell_exec('sysctl -n hw.perflevel0.logicalcpu 2>/dev/null') ?: 0);
        $effCores = (int) (@\shell_exec('sysctl -n hw.perflevel1.logicalcpu 2>/dev/null') ?: 0);

        $childShare = (int) ($fileSize * 0.95);
        $children = $workers - 1;
        $bh = fopen($inputPath, 'rb');
        $boundaries = [0];

        if ($perfCores > 0 && $effCores > 0) {
            // We are on Apple silicon
            // parent gets a perf slot, children get ($perfCores - 1) perf cores + $effCores efficiency cores
            $perfChildren = $perfCores - 1;
            $effChildren = $children - $perfChildren;
            // Perf is like ~1.7x faster than eff, so we weight the chunks accordingly
            $perfChunk = (int) ($childShare / ($perfChildren + $effChildren * 0.6));
            $effChunk = (int) ($perfChunk * 0.6);

            $offset = 0;
            // We spawn the perf core children
            for ($i = 0; $i < $perfChildren; $i++) {
                $offset += $perfChunk;
                fseek($bh, $offset);
                fgets($bh);
                $boundaries[] = ftell($bh);
            }
            // Then our remaining efficiency cores
            for ($i = 0; $i < $effChildren; $i++) {
                $offset += $effChunk;
                fseek($bh, $offset);
                fgets($bh);
                $boundaries[] = ftell($bh);
            }
        } else {
            // We are on Linux so we do normal weighting
            for ($i = 1; $i < $children; $i++) {
                fseek($bh, (int) ($childShare * $i / $children));
                fgets($bh);
                $boundaries[] = ftell($bh);
            }
            fseek($bh, $childShare);
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fclose($bh);
        $boundaries[] = $fileSize;

        $gridSize = $slugCount * $dateCount * 2;

        // Fork workers, IPC via shmop (or temp files as fallback)
        $useShmop = \function_exists('shmop_open');
        $childMap = [];

        for ($w = 0; $w < $workers - 1; $w++) {
            $handle = $useShmop
                ? \shmop_open(0, 'c', 0600, $gridSize)
                : sys_get_temp_dir() . '/p100m_' . getmypid() . '_' . $w;

            $pid = pcntl_fork();
            if ($pid === 0) {
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugBases, $dateIds, $slugCount, $dateCount,
                );
                $packed = pack('v*', ...$wCounts);
                if ($useShmop) {
                    \shmop_write($handle, $packed, 0);
                } else {
                    file_put_contents($handle, $packed);
                }
                exit(0);
            }
            $childMap[$pid] = $handle;
        }

        // Parent processes last chunk while children work
        $counts = $this->parseRange(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $slugBases, $dateIds, $slugCount, $dateCount,
        );

        // We merge the worker results back once they finish (chunked unpacking + skip-zero)
        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }
            if (!isset($childMap[$pid])) continue;
            $handle = $childMap[$pid];

            if ($useShmop) {
                $data = \shmop_read($handle, 0, $gridSize);
                \shmop_delete($handle);
            } else {
                $data = file_get_contents($handle);
                unlink($handle);
            }

            for ($pos = 0; $pos < $gridSize; $pos += 16384) {
                $sz = $gridSize - $pos;
                if ($sz > 16384) $sz = 16384;
                $n = $sz >> 1;
                $batch = unpack("v{$n}", $data, $pos);
                $idx = $pos >> 1;
                foreach ($batch as $v) {
                    if ($v) $counts[$idx] += $v;
                    $idx++;
                }
            }
            $pending--;
        }

        $this->writeJson($outputPath, $counts, $slugLabels, $dateLabels, $dateCount);
    }

    /**
     * @return int[]
     */
    private function parseRange(
        string $inputPath,
        int $start,
        int $end,
        array $slugBases,
        array $dateIds,
        int $slugCount,
        int $dateCount,
    ): array {
        $counts = array_fill(0, $slugCount * $dateCount, 0);
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = fread($fh, $toRead);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            // Manual unrolling of loop because idk?
            $p = 25;
            $fence = $lastNl - 960;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $counts[$slugBases[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$slugBases[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$slugBases[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$slugBases[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$slugBases[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$slugBases[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$slugBases[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$slugBases[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $counts[$slugBases[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }
        }

        fclose($fh);

        return $counts;
    }

    private function writeJson(
        string $outputPath,
        array $counts,
        array $slugLabels,
        array $dateLabels,
        int $dateCount,
    ): void {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePfx = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePfx[$d] = '        "20' . $dateLabels[$d] . '": ';
        }

        $slugCount = count($slugLabels);
        $slugHdr = [];
        for ($s = 0; $s < $slugCount; $s++) {
            $slugHdr[$s] = '"\\/blog\\/' . str_replace('/', '\\/', $slugLabels[$s]) . '"';
        }

        $first = true;

        for ($s = 0; $s < $slugCount; $s++) {
            $base = $s * $dateCount;
            $parts = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $cnt = $counts[$base + $d];
                if ($cnt === 0) continue;
                $parts[] = $datePfx[$d] . $cnt;
            }

            if (!$parts) continue;

            fwrite($out, ($first ? '' : ',') . "\n    " . $slugHdr[$s] . ": {\n" . implode(",\n", $parts) . "\n    }");
            $first = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
