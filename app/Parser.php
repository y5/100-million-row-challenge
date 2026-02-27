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

        // "YY-MM-DD" => integer ID for lookups
        $dateIds = [];
        $dateLabels = [];
        $dateCount = 0;

        for ($y = 20; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $y . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dateLabels[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        // We do a lil file probing business to get the slugs
        // while preserving first-seen order
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, $fileSize > self::PROBE_SIZE ? self::PROBE_SIZE : $fileSize);
        fclose($handle);

        $slugBases = [];
        $slugLabels = [];
        $slugCount = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n");

        while ($pos < $lastNl) {
            $nlPos = strpos($raw, "\n", $pos + 52);
            if ($nlPos === false) break;
            $slug = substr($raw, $pos + 25, $nlPos - $pos - 51);
            if (!isset($slugBases[$slug])) {
                $slugBases[$slug] = $slugCount * $dateCount;
                $slugLabels[$slugCount] = $slug;
                $slugCount++;
            }
            $pos = $nlPos + 1;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($slugBases[$slug])) {
                $slugBases[$slug] = $slugCount * $dateCount;
                $slugLabels[$slugCount] = $slug;
                $slugCount++;
            }
        }

        // We do some funny worker scaling business
        // $cpus = (int) (@\shell_exec('nproc 2>/dev/null') ?: @\shell_exec('sysctl -n hw.ncpu 2>/dev/null') ?: 8);
        // $workers = (int) \max(8, \ceil($cpus * 1.25));
        $workers = 8;

        // Asymmetric split: Parent process gets ~5% of work so it finished early and starts merging results
        $childShare = (int) ($fileSize * 0.95);
        $boundaries = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workers - 1; $i++) {
            fseek($bh, (int) ($childShare * $i / ($workers - 1)));
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fseek($bh, $childShare);
        fgets($bh);
        $boundaries[] = ftell($bh);
        fclose($bh);
        $boundaries[] = $fileSize;

        $gridSize = $slugCount * $dateCount * 2;

        // Fork workers, shmopping it hardcore
        $childMap = [];
        $tmpDir = '';
        $myPid = 0;

        for ($w = 0; $w < $workers - 1; $w++) {
            $handle = \shmop_open(0, 'c', 0600, $gridSize);

            $pid = pcntl_fork();
            if ($pid === 0) {
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugBases, $dateIds, $slugCount, $dateCount,
                );
                $packed = pack('v*', ...$wCounts);
                \shmop_write($handle, $packed, 0);
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

            $data = \shmop_read($handle, 0, $gridSize);
            \shmop_delete($handle);

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
