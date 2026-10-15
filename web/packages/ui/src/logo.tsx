/**
 * The Loams mark and wordmark (the design system page, loam-cloud design/index.html). The mark is an L built
 * from strata: a stem (the log, and a root), a bedrock (object storage),
 * layers that widen as they settle (compaction), and one ochre seed on top
 * (the newest write). The wordmark, "loams", is Archivo at width 118 and
 * weight 616 with -9.6 units of tracking, outlined, so it renders the same
 * without the font.
 */

type MarkProps = {
  size?: number;
  className?: string;
  mono?: boolean;
  /** Force the full cut; by default sizes under 24px get the favicon cut. */
  full?: boolean;
};

export function Mark({ size = 24, className, mono = false, full = false }: MarkProps) {
  // Below 24px the strata close up; the 16-unit favicon cut drops one.
  if (size < 24 && !full) {
    return (
      <svg width={size} height={size} viewBox="0 0 16 16" aria-hidden="true" className={className}>
        <rect x="1" y="1" width="4" height="14" fill="currentColor" />
        <rect x="1" y="11" width="14" height="4" fill="currentColor" />
        <rect x="7" y="7" width="6" height="2" fill="currentColor" />
        <rect x="7" y="1" width="3" height="3" fill={mono ? 'currentColor' : 'var(--op-accent)'} />
      </svg>
    );
  }
  return (
    <svg width={size} height={size} viewBox="3 3 26 26" aria-hidden="true" className={className}>
      <rect x="3" y="3" width="6" height="26" fill="currentColor" />
      <rect x="3" y="23" width="26" height="6" fill="currentColor" />
      <rect x="12" y="16" width="14" height="4" fill="currentColor" />
      <rect x="12" y="10" width="9" height="3" fill="currentColor" />
      <rect x="12" y="3" width="4" height="4" fill={mono ? 'currentColor' : 'var(--op-accent)'} />
    </svg>
  );
}

/** Width of the wordmark's view box: "loams", ending at the right edge of the s. */
const WORDMARK_WIDTH = 3212;

const WORDMARK =
  'M-0.1 736V12.2H137.1V736ZM549.7 748Q452.6 748 383.7 717.4Q314.9 686.8 278.6 625.5Q242.4 564.1 242.4 472.3Q242.4 379.4 278.6 318.5Q314.9 257.7 383.7 227.4Q452.6 197.2 549.7 197.2Q647.1 197.2 715.8 227.4Q784.5 257.7 820.7 318.5Q857 379.4 857 472.3Q857 564.1 820.7 625.5Q784.5 686.8 715.8 717.4Q647.1 748 549.7 748ZM549.7 641.5Q603.1 641.5 640.1 623.1Q677.1 604.7 696.1 569Q715.1 533.3 715.1 481.7V462.9Q715.1 410.6 696.1 375.2Q677.1 339.8 640.1 321.8Q603.1 303.8 549.7 303.8Q496.3 303.8 459.3 321.8Q422.3 339.8 403.3 375.2Q384.3 410.6 384.3 462.9V481.7Q384.3 533.3 403.3 569Q422.3 604.7 459.3 623.1Q496.3 641.5 549.7 641.5ZM1143.6 748Q1103.2 748 1065.9 741.1Q1028.6 734.1 999.2 717.3Q969.8 700.4 952.8 670.6Q935.8 640.7 935.8 594.9Q935.8 533.6 967.7 497.7Q999.5 461.8 1057.8 444.5Q1116 427.2 1194.6 422Q1273.2 416.7 1365.7 416.7V389.4Q1365.7 358.7 1351.9 339.4Q1338.1 320.1 1308.4 311.1Q1278.6 302.1 1228.8 302.1Q1187 302.1 1157 308.8Q1126.9 315.5 1110.9 327.7Q1095 339.9 1095 356.8V369H958.8Q957.8 364.8 957.5 360.6Q957.1 356.3 957.1 350.9Q957.1 304.4 989.4 269.8Q1021.6 235.3 1083.8 216.2Q1146 197.2 1236.4 197.2Q1323.9 197.2 1383.2 215Q1442.4 232.8 1472.6 271.3Q1502.8 309.7 1502.8 372.1V603.5Q1502.8 622.4 1511.3 631.9Q1519.7 641.5 1537 641.5H1582.9V731.1Q1572.6 735.3 1547.8 741.5Q1523.1 747.7 1491.9 747.7Q1452.5 747.7 1428.7 737.5Q1404.9 727.2 1393.1 709.2Q1381.3 691.1 1376.6 668.6H1369.2Q1344.2 693 1309.6 711.2Q1275 729.4 1233.1 738.7Q1191.2 748 1143.6 748ZM1180 642.1Q1210.8 642.1 1243.2 634.7Q1275.7 627.3 1303.6 613.3Q1331.5 599.3 1348.6 578.4Q1365.7 557.5 1365.7 530.7V504.1Q1270.4 504.1 1205.2 511.5Q1139.9 518.9 1106.8 537.1Q1073.6 555.2 1073.6 588.2Q1073.6 609 1088.5 620.6Q1103.3 632.3 1127.8 637.2Q1152.2 642.1 1180 642.1ZM1642.9 736V209.2H1754.4L1764.6 293.6H1773Q1797.9 258.9 1830 237.8Q1862.1 216.8 1899.3 207Q1936.5 197.2 1973.5 197.2Q2039.9 197.2 2085.3 220.3Q2130.8 243.3 2152.3 293.6H2160.8Q2186.3 258.9 2219.1 237.8Q2251.8 216.8 2289.4 207Q2327 197.2 2364.9 197.2Q2428.9 197.2 2473.3 218.7Q2517.7 240.2 2540.9 285.1Q2564.1 330.1 2564.1 400.9V736H2426.9V425.2Q2426.9 393.7 2418.9 371.8Q2410.9 350 2396.3 337Q2381.7 324 2361 318.2Q2340.3 312.4 2315.3 312.4Q2277.3 312.4 2244.6 329.7Q2211.9 347 2191.9 377.6Q2171.9 408.1 2171.9 449V736H2035.4V425.2Q2035.4 393.7 2027.4 371.8Q2019.4 350 2004.7 337Q1990.1 324 1969.4 318.2Q1948.8 312.4 1923.1 312.4Q1885 312.4 1852.2 329.7Q1819.4 347 1799.7 377.6Q1780.1 408.1 1780.1 449V736ZM2933.6 748Q2875.6 748 2825.6 737.5Q2775.6 727 2737.6 707.5Q2699.6 688 2678.6 657.5Q2657.6 627 2657.6 587Q2657.6 582 2658.1 578Q2658.6 574 2659.6 571L2797.6 571Q2797.6 572 2797.1 574.5Q2796.6 577 2796.6 579Q2797.6 605 2817.6 620Q2837.6 635 2868.6 641.5Q2899.6 648 2935.6 648Q2970.6 648 3002.6 643.5Q3034.6 639 3054.6 627Q3074.6 615 3074.6 591Q3074.6 565 3051.1 553Q3027.6 541 2986.6 534Q2945.6 527 2887.6 518Q2844.6 510 2806.1 499Q2767.6 488 2738.1 471Q2708.6 454 2692.1 426.5Q2675.6 399 2675.6 359Q2675.6 316 2694.6 286Q2713.6 256 2748.6 236.5Q2783.6 217 2832.6 207.5Q2881.6 198 2941.6 198Q2996.6 198 3042.6 207.5Q3088.6 217 3122.6 236Q3156.6 255 3176.1 282.5Q3195.6 310 3195.6 345Q3195.6 349 3195.6 353.5Q3195.6 358 3194.6 362L3057.6 362L3057.6 357Q3057.6 336 3041.1 322.5Q3024.6 309 2998.6 303Q2972.6 297 2939.6 297Q2924.6 297 2902.6 298.5Q2880.6 300 2860.1 305Q2839.6 310 2825.6 319.5Q2811.6 329 2811.6 345Q2811.6 366 2834.1 377Q2856.6 388 2892.6 394Q2928.6 400 2970.6 407Q3017.6 415 3061.6 424.5Q3105.6 434 3138.6 450.5Q3171.6 467 3191.6 496.5Q3211.6 526 3211.6 574Q3211.6 623 3192.1 656.5Q3172.6 690 3137.1 710Q3101.6 730 3049.6 739Q2997.6 748 2933.6 748Z';

export function Wordmark({ height = 20, className }: { height?: number; className?: string }) {
  return (
    <svg
      height={height}
      width={(height * WORDMARK_WIDTH) / 748}
      viewBox={`0 0 ${WORDMARK_WIDTH} 748`}
      aria-hidden="true"
      className={className}
    >
      <path fill="currentColor" d={WORDMARK} />
    </svg>
  );
}

/** Mark and wordmark, the mark as tall as the "l" and the gap 0.285 of it. */
export function Logo({ height = 20, mono = false }: { height?: number; mono?: boolean }) {
  return (
    <span
      className="loams-logo"
      style={{ gap: height * 0.285, height }}
      role="img"
      aria-label="Loams"
    >
      <Mark size={height} mono={mono} full={height >= 18} />
      <Wordmark height={height} />
    </span>
  );
}
