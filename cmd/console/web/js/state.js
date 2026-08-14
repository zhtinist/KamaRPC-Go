// 状态层: 保存上一次采样、把累计计数换算成速率、维护图表历史。
// 这一层同样不碰 DOM —— 视图只消费它算好的结果。

/** 图表最多保留多少个点 */
export const MAX_POINTS = 120;

export const state = {
  intervalMs: 1000,
  running: true,
  prev: null, // 上一次的 overview 响应
  qpsHistory: [],
  lastError: null,
};

function methodKey(target, m) {
  return `${target}|${m.service}.${m.method}`;
}

/**
 * 把两次采样的累计计数差分成速率。
 *
 * 采集端只记累计值, 速率在这里算 —— 好处是采集端不必维护时间窗口, 也不会
 * 因为面板改了刷新间隔就失真; 代价是第一次采样出不来速率(没有前一个点)。
 *
 * 计数变小(服务端重启, 计数从头开始)时按 0 处理, 不能出现负速率。
 */
export function computeRates(cur, prev) {
  const rates = new Map();
  let totalQPS = 0;
  let totalErrRate = 0;

  if (!prev) return { rates, totalQPS, totalErrRate, ready: false };

  const dt = (cur.timestampMs - prev.timestampMs) / 1000;
  if (!(dt > 0)) return { rates, totalQPS, totalErrRate, ready: false };

  const before = new Map();
  for (const t of prev.targets ?? []) {
    if (!t.ok || !t.stats) continue;
    for (const m of t.stats.methods ?? []) before.set(methodKey(t.target, m), m);
  }

  for (const t of cur.targets ?? []) {
    if (!t.ok || !t.stats) continue;
    for (const m of t.stats.methods ?? []) {
      const prevM = before.get(methodKey(t.target, m));
      if (!prevM) continue;

      const qps = Math.max(0, (m.count - prevM.count) / dt);
      const errRate = Math.max(0, (m.errors - prevM.errors) / dt);
      rates.set(methodKey(t.target, m), { qps, errRate });
      totalQPS += qps;
      totalErrRate += errRate;
    }
  }

  return { rates, totalQPS, totalErrRate, ready: true };
}

/** 从一次 overview 里汇总出概览数字 */
export function summarize(data, totalQPS, totalErrRate) {
  const targets = data.targets ?? [];
  const topology = data.topology ?? [];

  const instances = new Set();
  for (const s of topology) for (const addr of s.instances ?? []) instances.add(addr);

  let connections = 0;
  let goroutines = 0;
  let totalCalls = 0;
  let totalErrors = 0;

  for (const t of targets) {
    if (!t.ok || !t.stats) continue;
    connections += t.stats.connections ?? 0;
    goroutines += t.stats.goroutines ?? 0;
    for (const m of t.stats.methods ?? []) {
      totalCalls += m.count ?? 0;
      totalErrors += m.errors ?? 0;
    }
  }

  return {
    totalQPS,
    errorRatio: totalQPS > 0 ? totalErrRate / totalQPS : 0,
    liveTargets: targets.filter((t) => t.ok).length,
    allTargets: targets.length,
    registeredInstances: instances.size,
    connections,
    goroutines,
    totalCalls,
    totalErrors,
  };
}

export function pushQPS(value) {
  if (state.qpsHistory.length >= MAX_POINTS) state.qpsHistory.shift();
  state.qpsHistory.push(value);
}
