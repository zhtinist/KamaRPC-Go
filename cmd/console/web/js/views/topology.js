// 视图: 服务拓扑(来自 etcd)
//
// 服务名与实例地址都是外部数据(任何能写注册中心的进程都能左右它),
// 所以一律经 esc() 转义后再拼进 innerHTML。
import { esc } from '../format.js';

export function renderTopology(el, topology) {
  if (!topology?.length) {
    el.innerHTML = '<div class="empty">没有配置要展示的服务（见 console 的 -services 参数）</div>';
    return;
  }

  const rows = topology
    .map((s) => {
      const instances = s.instances ?? [];
      const pills = instances.length
        ? instances.map((addr) => `<span class="pill">${esc(addr)}</span>`).join('')
        : '<span class="pill down">无可用实例</span>';
      const err = s.error ? `<div class="err-text">${esc(s.error)}</div>` : '';

      return `<tr>
        <td class="strong">${esc(s.service)}</td>
        <td>${pills}${err}</td>
        <td class="num">${instances.length}</td>
      </tr>`;
    })
    .join('');

  el.innerHTML = `
    <table>
      <thead><tr><th>服务</th><th>实例</th><th class="num">数量</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}
