// 视图: 服务端实例状态(抓取结果)
import { esc, num, uptime } from '../format.js';

export function renderTargets(el, targets) {
  if (!targets?.length) {
    el.innerHTML = '<div class="empty">没有配置抓取目标（见 console 的 -targets 参数）</div>';
    return;
  }

  const rows = targets
    .map((t) => {
      if (!t.ok) {
        // 抓不到时把具体错误显示出来, 比只标红更有用: 是连不上、超时还是返回了坏 JSON
        return `<tr class="row-down">
          <td><span class="dot down"></span>${esc(t.target)}</td>
          <td colspan="4" class="err-text">${esc(t.error || '抓取失败')}</td>
        </tr>`;
      }

      const s = t.stats ?? {};
      const services = (s.services ?? []).map((x) => x.name);
      return `<tr>
        <td>
          <span class="dot up"></span>${esc(t.target)}
          <div class="sub-line">${esc(s.addr ?? '')} · ${services.map(esc).join(' / ') || '无服务'}</div>
        </td>
        <td class="num">${num(s.connections)}</td>
        <td class="num">${num(s.goroutines)}</td>
        <td class="num">${uptime(s.uptimeSec)}</td>
        <td class="num">${num(s.pid)}</td>
      </tr>`;
    })
    .join('');

  el.innerHTML = `
    <table>
      <thead><tr>
        <th>指标接口</th><th class="num">连接</th><th class="num">协程</th>
        <th class="num">运行时长</th><th class="num">PID</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}
