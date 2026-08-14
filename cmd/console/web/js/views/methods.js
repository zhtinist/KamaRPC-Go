// 视图: 方法级指标表(服务.方法 × 实例)
import { esc, num, pct } from '../format.js';

export function renderMethods(el, targets, rates) {
  const rows = [];

  for (const t of targets ?? []) {
    if (!t.ok || !t.stats) continue;

    const methods = [...(t.stats.methods ?? [])].sort((a, b) => b.count - a.count);
    for (const m of methods) {
      const rate = rates.get(`${t.target}|${m.service}.${m.method}`);
      const errRatio = m.count > 0 ? m.errors / m.count : 0;

      rows.push(`<tr>
        <td class="strong">${esc(m.service)}.${esc(m.method)}</td>
        <td class="dim">${esc(t.target)}</td>
        <td class="num">${rate ? num(rate.qps) : '—'}</td>
        <td class="num">${num(m.count)}</td>
        <td class="num ${errRatio > 0.01 ? 'bad' : ''}">${pct(errRatio)}</td>
        <td class="num">${num(m.avgMs, 2)}</td>
        <td class="num">${num(m.p50Ms, 2)}</td>
        <td class="num">${num(m.p90Ms, 2)}</td>
        <td class="num">${num(m.p99Ms, 2)}</td>
      </tr>`);
    }
  }

  if (!rows.length) {
    el.innerHTML =
      '<div class="empty">还没有调用记录 —— 跑一下 <code>cmd/client</code> 或压测程序就会出现</div>';
    return;
  }

  el.innerHTML = `
    <table>
      <thead><tr>
        <th>方法</th><th>实例</th>
        <th class="num">QPS</th><th class="num">累计</th><th class="num">错误率</th>
        <th class="num">平均(ms)</th><th class="num">P50</th><th class="num">P90</th><th class="num">P99</th>
      </tr></thead>
      <tbody>${rows.join('')}</tbody>
    </table>`;
}
