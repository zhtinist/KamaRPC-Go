// 视图: 客户端治理状态(熔断器 + 连接池)
//
// 这一块是服务端视角看不到的: 被限流或熔断挡掉的调用根本没上网络,
// 熔断器的三态也只存在于调用方进程里。
import { esc, num, uptime } from '../format.js';

const STATE_LABEL = {
  closed: { text: '闭合', cls: 'ok' },
  open: { text: '熔断', cls: 'bad' },
  'half-open': { text: '半开探测', cls: 'warn' },
};

function breakerRows(clientName, breakers) {
  return breakers
    .map((b) => {
      const s = STATE_LABEL[b.state] ?? { text: b.state, cls: '' };
      return `<tr>
        <td class="strong">${esc(b.service)}</td>
        <td class="dim">${esc(b.addr)}</td>
        <td><span class="badge ${s.cls}">${esc(s.text)}</span></td>
        <td class="num">${num(b.failures)}</td>
        <td class="num">${num(b.successes)}</td>
        <td class="dim">${esc(clientName)}</td>
      </tr>`;
    })
    .join('');
}

export function renderBreakers(el, clients) {
  const rows = [];
  for (const c of clients ?? []) {
    if (!c.ok || !c.stats) continue;
    rows.push(breakerRows(c.stats.name ?? c.target, c.stats.breakers ?? []));
  }

  const body = rows.join('');
  if (!body) {
    el.innerHTML =
      '<div class="empty">还没有熔断器 —— 客户端发起调用后，会按「服务@实例」维度各建一个</div>';
    return;
  }

  el.innerHTML = `
    <table>
      <thead><tr>
        <th>服务</th><th>实例</th><th>状态</th>
        <th class="num">窗口失败</th><th class="num">窗口成功</th><th>调用方</th>
      </tr></thead>
      <tbody>${body}</tbody>
    </table>`;
}

export function renderClients(el, clients) {
  if (!clients?.length) {
    el.innerHTML = '<div class="empty">没有配置客户端指标接口（见 console 的 -clients 参数）</div>';
    return;
  }

  const rows = clients
    .map((c) => {
      if (!c.ok) {
        return `<tr class="row-down">
          <td><span class="dot down"></span>${esc(c.target)}</td>
          <td colspan="4" class="err-text">${esc(c.error || '抓取失败')}</td>
        </tr>`;
      }

      const s = c.stats ?? {};
      const pools = (s.pools ?? [])
        .map(
          (p) =>
            `<span class="pill ${p.closed ? 'down' : ''}">${esc(p.addr)} ${p.active}/${p.maxActive}</span>`,
        )
        .join('');

      return `<tr>
        <td><span class="dot up"></span>${esc(s.name ?? c.target)}
          <div class="sub-line">${esc(c.target)} · PID ${num(s.pid)} · ${uptime(s.uptimeSec)}</div>
        </td>
        <td>${pools || '<span class="dim">尚未建连</span>'}</td>
        <td class="num">${num((s.breakers ?? []).length)}</td>
        <td class="num">${num(s.goroutines)}</td>
        <td class="num">${num((s.methods ?? []).reduce((a, m) => a + (m.count ?? 0), 0))}</td>
      </tr>`;
    })
    .join('');

  el.innerHTML = `
    <table>
      <thead><tr>
        <th>调用方</th><th>连接池（已建/上限）</th>
        <th class="num">熔断器</th><th class="num">协程</th><th class="num">累计调用</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}
