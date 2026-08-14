// 入口: 只做编排 —— 调度轮询, 把 api 拿到的数据交给 state 换算, 再分发给各视图。
// 具体怎么取数、怎么算、怎么画, 分别在 api / state / views / chart 里。

import { fetchOverview } from './api.js';
import { computeRates, pushQPS, state, summarize, MAX_POINTS } from './state.js';
import { drawSeries } from './chart.js';
import { timeOfDay } from './format.js';
import { renderSummary } from './views/summary.js';
import { renderTopology } from './views/topology.js';
import { renderTargets } from './views/targets.js';
import { renderMethods } from './views/methods.js';

const el = {
  summary: document.getElementById('summary'),
  chart: document.getElementById('qpsChart'),
  topology: document.getElementById('topology'),
  targets: document.getElementById('targets'),
  methods: document.getElementById('methods'),
  status: document.getElementById('status'),
  interval: document.getElementById('interval'),
  toggle: document.getElementById('toggle'),
};

let timer = null;
let inFlight = false; // 防止上一轮还没回来就又发一轮

function setStatus(text, tone = '') {
  el.status.textContent = text;
  el.status.className = `status ${tone}`;
}

async function tick() {
  // 一轮没结束就跳过这次触发: 后端慢的时候不该把请求越堆越多
  if (inFlight) return;
  inFlight = true;

  try {
    const data = await fetchOverview({ timeoutMs: Math.max(3000, state.intervalMs * 3) });

    const { rates, totalQPS, totalErrRate, ready } = computeRates(data, state.prev);
    state.prev = data;
    if (ready) pushQPS(totalQPS);

    renderSummary(el.summary, summarize(data, totalQPS, totalErrRate), ready);
    renderTopology(el.topology, data.topology);
    renderTargets(el.targets, data.targets);
    renderMethods(el.methods, data.targets, rates);
    drawSeries(el.chart, state.qpsHistory, { maxPoints: MAX_POINTS });

    state.lastError = null;
    setStatus(`已更新 ${timeOfDay()}`, 'ok');
  } catch (err) {
    state.lastError = err;
    setStatus(`获取失败: ${err.message}`, 'err');
  } finally {
    inFlight = false;
  }
}

function schedule() {
  clearInterval(timer);
  timer = null;
  if (!state.running) return;
  tick();
  timer = setInterval(tick, state.intervalMs);
}

el.interval.addEventListener('change', (e) => {
  state.intervalMs = Number(e.target.value);
  schedule();
});

el.toggle.addEventListener('click', () => {
  state.running = !state.running;
  el.toggle.textContent = state.running ? '暂停' : '继续';
  el.toggle.classList.toggle('paused', !state.running);
  if (state.running) {
    schedule();
  } else {
    clearInterval(timer);
    setStatus('已暂停', '');
  }
});

// 标签页切到后台就别轮询了 —— 白跑的请求既占后端也让抓取曲线出现无意义的空档
document.addEventListener('visibilitychange', () => {
  if (!state.running) return;
  if (document.hidden) {
    clearInterval(timer);
    timer = null;
  } else {
    schedule();
  }
});

// 窗口尺寸变了只需重画图, 不必重新取数
window.addEventListener('resize', () => drawSeries(el.chart, state.qpsHistory, { maxPoints: MAX_POINTS }));

schedule();
