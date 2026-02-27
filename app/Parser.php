<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function count;
use function fclose;
use function fgets;
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
    private const int READ_CHUNK = 1_048_576;
    private const int PROBE_SIZE = 2_097_152;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);

        // Map every valid "YY-MM-DD" to a sequential integer
        $dpm = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        $dateLabels = [];
        $dateCount = 0;

        // Character-indexed date lookup: avoids substr + hash in the hot loop
        // For years 2020-2026, tens digit is always '2', so units char determines year
        // $ymBase[yearUnitChar][monthTensChar][monthUnitChar] = cumulative day offset
        // $dayLookup[dayTensChar][dayUnitChar] = 0-based day
        $ymBase = [];
        $dayLookup = [];

        for ($year = 2020; $year <= 2026; $year++) {
            $yChar = \chr(($year - 2020) + 48);
            $ymBase[$yChar] = [];
            for ($m = 0; $m < 12; $m++) {
                $days = $dpm[$m] + ($m === 1 && $year % 4 === 0 ? 1 : 0);
                $mtChar = \chr(\intdiv($m + 1, 10) + 48);
                $muChar = \chr(($m + 1) % 10 + 48);
                $ymBase[$yChar][$mtChar][$muChar] = $dateCount;
                $prefix = sprintf('%d-%02d-', $year - 2000, $m + 1);
                for ($d = 1; $d <= $days; $d++) {
                    $dateLabels[$dateCount] = $prefix . sprintf('%02d', $d);
                    $dateCount++;
                }
            }
        }

        for ($d = 1; $d <= 31; $d++) {
            $dayLookup[\chr(\intdiv($d, 10) + 48)][\chr($d % 10 + 48)] = $d - 1;
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

        // Major lookup alert: we wanna identify slugs by length + 2 char positions
        // $slugFast[len][char@p1][char@p2] = base, $slugP1/$slugP2 = which positions to check
        // For unique lengths, $slugFast[len] = base directly (int not array)
        $byLen = [];
        foreach ($slugBases as $slug => $base) {
            $byLen[strlen($slug)][] = [$slug, $base];
        }

        $slugFast = [];
        $slugP1 = [];
        $slugP2 = [];

        foreach ($byLen as $len => $entries) {
            if (count($entries) === 1) {
                $slugFast[$len] = $entries[0][1];
                continue;
            }

            for ($p1 = 0; $p1 < $len; $p1++) {
                for ($p2 = $p1 + 1; $p2 < $len; $p2++) {
                    $keys = [];
                    $unique = true;
                    foreach ($entries as [$s, $b]) {
                        $k = $s[$p1] . $s[$p2];
                        if (isset($keys[$k])) { $unique = false; break; }
                        $keys[$k] = true;
                    }
                    if ($unique) {
                        $slugP1[$len] = $p1;
                        $slugP2[$len] = $p2;
                        $slugFast[$len] = [];
                        foreach ($entries as [$s, $b]) {
                            $slugFast[$len][$s[$p1]][$s[$p2]] = $b;
                        }
                        goto nextLen;
                    }
                }
            }
            // Fallback for lengths needing 3+ positions: keep as string-keyed map
            $slugP1[$len] = -1;
            $slugP2[$len] = 0;
            $slugFast[$len] = [];
            foreach ($entries as [$s, $b]) {
                $slugFast[$len][$s] = $b;
            }
            nextLen:
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

        // Fork workers, IPC via temp files (tmpfs on Linux = RAM, no disk I/O)
        $childMap = [];
        $parentPid = getmypid();

        for ($w = 0; $w < $workers - 1; $w++) {
            $tmpFile = sys_get_temp_dir() . '/p100m_' . $parentPid . '_' . $w;

            $pid = pcntl_fork();
            if ($pid === 0) {
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugFast, $slugP1, $slugP2, $ymBase, $dayLookup, $slugCount, $dateCount,
                );
                \file_put_contents($tmpFile, pack('v*', ...$wCounts));
                exit(0);
            }
            $childMap[$pid] = $tmpFile;
        }

        // Parent processes last chunk while children work
        $counts = $this->parseRange(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $slugFast, $slugP1, $slugP2, $ymBase, $dayLookup, $slugCount, $dateCount,
        );

        // Merge worker results as they finish
        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }
            if (!isset($childMap[$pid])) continue;
            $tmpFile = $childMap[$pid];
            $data = \file_get_contents($tmpFile);
            unlink($tmpFile);

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
     * Parse a byte range of the input file and return a flat counts array.
     *
     * @return int[]
     */
    private function parseRange(
        string $inputPath,
        int $start,
        int $end,
        array $slugFast,
        array $slugP1,
        array $slugP2,
        array $ymBase,
        array $dayLookup,
        int $slugCount,
        int $dateCount,
    ): array {
        $totalCells = $slugCount * $dateCount;
        $counts = array_fill(0, $totalCells, 0);
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);
        $bytesLeft = $end - $start;

        while ($bytesLeft > 0) {
            $readSize = $bytesLeft > self::READ_CHUNK ? self::READ_CHUNK : $bytesLeft;
            $buf = fread($fh, $readSize);
            $bufLen = strlen($buf);
            if ($bufLen === 0) break;
            $bytesLeft -= $bufLen;

            $endOfLastLine = strrpos($buf, "\n");
            if ($endOfLastLine === false) break;

            $overflow = $bufLen - $endOfLastLine - 1;
            if ($overflow > 0) {
                fseek($fh, -$overflow, SEEK_CUR);
                $bytesLeft += $overflow;
            }

            $cursor = 25;
            $safeEnd = $endOfLastLine - 480;

            // 4x unrolled hot loop
            // Slug lookup: length → unique base (int) or [char@p1][char@p2] → base
            // Date lookup: char-indexed year/month/day → dateId
            while ($cursor < $safeEnd) {
                $c = strpos($buf, ',', $cursor); $l = $c - $cursor; $f = $slugFast[$l];
                $counts[(\is_int($f) ? $f : ($slugP1[$l] >= 0 ? $f[$buf[$cursor + $slugP1[$l]]][$buf[$cursor + $slugP2[$l]]] : $f[substr($buf, $cursor, $l)])) + $ymBase[$buf[$c + 4]][$buf[$c + 6]][$buf[$c + 7]] + $dayLookup[$buf[$c + 9]][$buf[$c + 10]]]++;
                $cursor = $c + 52;

                $c = strpos($buf, ',', $cursor); $l = $c - $cursor; $f = $slugFast[$l];
                $counts[(\is_int($f) ? $f : ($slugP1[$l] >= 0 ? $f[$buf[$cursor + $slugP1[$l]]][$buf[$cursor + $slugP2[$l]]] : $f[substr($buf, $cursor, $l)])) + $ymBase[$buf[$c + 4]][$buf[$c + 6]][$buf[$c + 7]] + $dayLookup[$buf[$c + 9]][$buf[$c + 10]]]++;
                $cursor = $c + 52;

                $c = strpos($buf, ',', $cursor); $l = $c - $cursor; $f = $slugFast[$l];
                $counts[(\is_int($f) ? $f : ($slugP1[$l] >= 0 ? $f[$buf[$cursor + $slugP1[$l]]][$buf[$cursor + $slugP2[$l]]] : $f[substr($buf, $cursor, $l)])) + $ymBase[$buf[$c + 4]][$buf[$c + 6]][$buf[$c + 7]] + $dayLookup[$buf[$c + 9]][$buf[$c + 10]]]++;
                $cursor = $c + 52;

                $c = strpos($buf, ',', $cursor); $l = $c - $cursor; $f = $slugFast[$l];
                $counts[(\is_int($f) ? $f : ($slugP1[$l] >= 0 ? $f[$buf[$cursor + $slugP1[$l]]][$buf[$cursor + $slugP2[$l]]] : $f[substr($buf, $cursor, $l)])) + $ymBase[$buf[$c + 4]][$buf[$c + 6]][$buf[$c + 7]] + $dayLookup[$buf[$c + 9]][$buf[$c + 10]]]++;
                $cursor = $c + 52;
            }

            while ($cursor < $endOfLastLine) {
                $c = strpos($buf, ',', $cursor);
                if ($c === false || $c >= $endOfLastLine) break;
                $l = $c - $cursor; $f = $slugFast[$l];
                $counts[(\is_int($f) ? $f : ($slugP1[$l] >= 0 ? $f[$buf[$cursor + $slugP1[$l]]][$buf[$cursor + $slugP2[$l]]] : $f[substr($buf, $cursor, $l)])) + $ymBase[$buf[$c + 4]][$buf[$c + 6]][$buf[$c + 7]] + $dayLookup[$buf[$c + 9]][$buf[$c + 10]]]++;
                $cursor = $c + 52;
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
        stream_set_write_buffer($out, 262_144);

        // Build the full JSON into a buffer and flush in big chunks
        $buf = '{';
        $slugCount = count($slugLabels);
        $sep = '';

        for ($s = 0; $s < $slugCount; $s++) {
            $base = $s * $dateCount;

            // Scan for any non-zero date in this slug
            $hasAny = false;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$base + $d] !== 0) { $hasAny = true; break; }
            }
            if (!$hasAny) continue;

            $escapedSlug = str_replace('/', '\\/', $slugLabels[$s]);
            $buf .= $sep . "\n    \"\\/blog\\/" . $escapedSlug . "\": {\n";
            $sep = ',';

            $innerSep = '';
            for ($d = 0; $d < $dateCount; $d++) {
                $cnt = $counts[$base + $d];
                if ($cnt === 0) continue;
                $buf .= $innerSep . '        "20' . $dateLabels[$d] . '": ' . $cnt;
                $innerSep = ",\n";
            }
            $buf .= "\n    }";

            // Flush when buffer gets large
            if (strlen($buf) > 131_072) {
                fwrite($out, $buf);
                $buf = '';
            }
        }

        $buf .= "\n}";
        fwrite($out, $buf);
        fclose($out);
    }
}
