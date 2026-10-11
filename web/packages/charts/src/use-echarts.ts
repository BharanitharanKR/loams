import * as echarts from 'echarts';
import { type RefObject, useEffect, useRef } from 'react';

export type EChartsInstance = echarts.ECharts;

/**
 * Own one ECharts instance for the lifetime of `host`.
 *
 * Inits on mount, resizes with the box through a ResizeObserver (cards in a
 * grid resize without a window resize event), and disposes on unmount so a
 * page that mounts and drops charts does not leak canvases or listeners.
 */
export function useECharts(host: RefObject<HTMLDivElement | null>): RefObject<EChartsInstance | null> {
  const instance = useRef<EChartsInstance | null>(null);
  useEffect(() => {
    const el = host.current;
    if (!el) return;
    const chart = echarts.init(el, undefined, { renderer: 'canvas' });
    instance.current = chart;
    let observer: ResizeObserver | undefined;
    if (typeof ResizeObserver !== 'undefined') {
      observer = new ResizeObserver(() => {
        if (!chart.isDisposed()) chart.resize();
      });
      observer.observe(el);
    }
    return () => {
      observer?.disconnect();
      chart.dispose();
      instance.current = null;
    };
  }, [host]);
  return instance;
}
