// 视图: 顶部概览卡片
import { num, pct } from '../format.js';

export function renderSummary(el, s, ratesReady) {
  const cards = [
    { label: '总 QPS', value: ratesReady ? num(s.totalQPS) : '—', tone: '' },
    {
      label: '错误率',
      value: ratesReady ? pct(s.errorRatio) : '—',
      tone: s.errorRatio > 0.01 ? 'bad' : '',
    },
    {
      label: '服务端实例',
      value: `${s.liveTargets}/${s.allTargets}`,
      tone: s.liveTargets < s.allTargets ? 'bad' : 'good',
    },
    { label: 'etcd 注册实例', value: num(s.registeredInstances), tone: '' },
    {
      label: '熔断中',
      value: num(s.openBreakers),
      tone: s.openBreakers > 0 ? 'bad' : 'good',
    },
    { label: '活跃连接', value: num(s.connections), tone: '' },
    { label: '协程数', value: num(s.goroutines), tone: '' },
    { label: '累计调用', value: num(s.totalCalls), tone: '', small: true },
  ];

  el.innerHTML = cards
    .map(
      (c) => `
      <div class="card">
        <div class="card-label">${c.label}</div>
        <div class="card-value ${c.tone} ${c.small ? 'small' : ''}">${c.value}</div>
      </div>`,
    )
    .join('');
}
