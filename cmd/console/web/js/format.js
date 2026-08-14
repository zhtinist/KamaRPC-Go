// 纯函数层: 只做数据到字符串的转换, 不碰 DOM、不发请求。
// 这一层没有副作用, 所以最容易单独推理与复用。

/**
 * HTML 转义。
 *
 * 面板展示的服务名、方法名、错误信息都来自 etcd 与各服务端 —— 也就是说,
 * 任何能往注册中心写数据的进程都能影响这里的输出。所有外部字符串在拼进
 * innerHTML 之前必须过这一层, 否则一个名字里带标签的服务就能注入脚本。
 */
export function esc(v) {
  if (v === null || v === undefined) return '';
  return String(v)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

/** 千分位数字; 非法值统一显示为 - */
export function num(n, digits = 0) {
  if (n === null || n === undefined || Number.isNaN(n)) return '-';
  return Number(n).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

/** 大数字缩写, 用于图表坐标轴 */
export function compact(n) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(n >= 10_000_000 ? 0 : 1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(n >= 10_000 ? 0 : 1)}k`;
  return String(Math.round(n));
}

/** 秒 → 人类可读的运行时长 */
export function uptime(sec) {
  if (sec === null || sec === undefined || Number.isNaN(sec)) return '-';
  const s = Math.floor(sec);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}

/** 百分比, 输入是 0~1 的比例 */
export function pct(ratio, digits = 2) {
  if (!Number.isFinite(ratio)) return '-';
  return `${num(ratio * 100, digits)}%`;
}

export function timeOfDay(ms = Date.now()) {
  return new Date(ms).toLocaleTimeString('zh-CN', { hour12: false });
}
