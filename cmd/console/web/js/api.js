// 数据访问层: 只负责跟后端说话, 不关心怎么展示。
// 上层拿到的要么是解析好的数据, 要么是一个带可读信息的 Error。

const DEFAULT_TIMEOUT_MS = 5000;

/**
 * 带超时的 GET JSON。
 *
 * 必须有超时: 面板是定时轮询的, 一个卡住的请求会一直占着, 后面每一轮都
 * 排在它后面, 表现就是"面板不动了却没有报错"。
 */
async function getJSON(path, { timeoutMs = DEFAULT_TIMEOUT_MS, signal } = {}) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(new Error('请求超时')), timeoutMs);

  // 调用方的取消信号与超时信号合并
  if (signal) signal.addEventListener('abort', () => ctrl.abort(signal.reason), { once: true });

  try {
    const res = await fetch(path, { cache: 'no-store', signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`.trim());
    return await res.json();
  } catch (err) {
    if (err?.name === 'AbortError') throw new Error(`请求超时(${timeoutMs}ms)`);
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

/** 拓扑 + 各实例指标, 一次拿全 */
export function fetchOverview(opts) {
  return getJSON('/api/overview', opts);
}

/** 只要 etcd 里的服务拓扑 */
export function fetchTopology(opts) {
  return getJSON('/api/topology', opts);
}

/** 只要各服务端抓来的指标 */
export function fetchStats(opts) {
  return getJSON('/api/stats', opts);
}
