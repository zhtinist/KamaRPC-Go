// KamaRPC 控制台前端。
//
// 刻意不用任何框架与 CDN: 页面只做三件事 —— 轮询 /api/overview、
// 把累计计数差分成速率、把结果画出来。没有构建步骤, 改完刷新即可。

const state = {
  timer: null,
  intervalMs: 1000,
  running: true,
  prev: null,        // 上一次采样(用于把累计值差分成 QPS)
  qpsHistory: [],    // 总 QPS 历史, 画折线用
  maxPoints: 120,
};

const $ = (id) => document.getElementById(id);

function fmt(n, digits = 0) {
  if (n === undefined || n === null || Number.isNaN(n)) return '-';
  return n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function fmtUptime(sec) {
  if (!sec && sec !== 0) return '-';
  const s = Math.floor(sec);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

// 把两次采样的累计计数差分成速率
function computeRates(cur, prev) {
  const rates = new Map();
  let totalQPS = 0, totalErrRate = 0;
  if (!prev) return { rates, totalQPS, totalErrRate };

  const dt = (cur.timestampMs - prev.timestampMs) / 1000;
  if (dt <= 0) return { rates, totalQPS, totalErrRate };

  const prevIndex = new Map();
  for (const t of prev.targets) {
    if (!t.ok || !t.stats) continue;
    for (const m of t.stats.methods || []) {
      prevIndex.set(`${t.target}|${m.service}.${m.method}`, m);
    }
  }

  for (const t of cur.targets) {
    if (!t.ok || !t.stats) continue;
    for (const m of t.stats.methods || []) {
      const key = `${t.target}|${m.service}.${m.method}`;
      const before = prevIndex.get(key);
      if (!before) continue;
      const qps = Math.max(0, (m.count - before.count) / dt);
      const errs = Math.max(0, (m.errors - before.errors) / dt);
      rates.set(key, { qps, errs });
      totalQPS += qps;
      totalErrRate += errs;
    }
  }
  return { rates, totalQPS, totalErrRate };
}

function renderSummary(data, totalQPS, totalErrRate) {
  const up = data.targets.filter((t) => t.ok).length;
  const conns = data.targets.reduce((a, t) => a + (t.ok ? t.stats.connections : 0), 0);
  const goroutines = data.targets.reduce((a, t) => a + (t.ok ? t.stats.goroutines : 0), 0);
  const calls = data.targets.reduce(
    (a, t) => a + (t.ok ? (t.stats.methods || []).reduce((b, m) => b + m.count, 0) : 0), 0);
  const instances = new Set();
  for (const s of data.topology) for (const i of s.instances) instances.add(i);

  const cards = [
    { label: '总 QPS', value: fmt(totalQPS), cls: '' },
    { label: '错误率', value: totalQPS > 0 ? `${fmt((totalErrRate / totalQPS) * 100, 2)}%` : '0%', cls: '' },
    { label: '服务端实例', value: `${up}/${data.targets.length}`, cls: '' },
    { label: 'etcd 注册实例', value: fmt(instances.size), cls: '' },
    { label: '活跃连接', value: fmt(conns), cls: '' },
    { label: '协程数', value: fmt(goroutines), cls: '' },
    { label: '累计调用', value: fmt(calls), cls: 'small' },
  ];

  $('summary').innerHTML = cards.map((c) => `
    <div class="card">
      <div class="label">${c.label}</div>
      <div class="value ${c.cls}">${c.value}</div>
    </div>`).join('');
}

function renderTopology(topology) {
  if (!topology.length) {
    $('topology').innerHTML = '<div class="empty">没有配置要展示的服务</div>';
    return;
  }
  const rows = topology.map((s) => {
    const pills = s.instances.length
      ? s.instances.map((i) => `<span class="pill">${i}</span>`).join('')
      : '<span class="pill down">无可用实例</span>';
    const err = s.error ? `<div class="err-text">${s.error}</div>` : '';
    return `<tr><td>${s.service}</td><td>${pills}${err}</td><td class="num">${s.instances.length}</td></tr>`;
  }).join('');
  $('topology').innerHTML =
    `<table><thead><tr><th>服务</th><th>实例</th><th class="num">数量</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function renderTargets(targets) {
  const rows = targets.map((t) => {
    if (!t.ok) {
      return `<tr><td><span class="dot down"></span>${t.target}</td>
        <td colspan="4" class="err-text">${t.error || '抓取失败'}</td></tr>`;
    }
    const s = t.stats;
    return `<tr>
      <td><span class="dot up"></span>${t.target}</td>
      <td class="num">${fmt(s.connections)}</td>
      <td class="num">${fmt(s.goroutines)}</td>
      <td class="num">${fmtUptime(s.uptimeSec)}</td>
      <td class="num">${fmt(s.pid)}</td>
    </tr>`;
  }).join('');
  $('targets').innerHTML = `<table><thead><tr>
      <th>指标接口</th><th class="num">连接</th><th class="num">协程</th>
      <th class="num">运行时长</th><th class="num">PID</th>
    </tr></thead><tbody>${rows}</tbody></table>`;
}

function renderMethods(data, rates) {
  const rows = [];
  for (const t of data.targets) {
    if (!t.ok || !t.stats) continue;
    for (const m of (t.stats.methods || []).slice().sort((a, b) => b.count - a.count)) {
      const r = rates.get(`${t.target}|${m.service}.${m.method}`);
      const errPct = m.count > 0 ? (m.errors / m.count) * 100 : 0;
      rows.push(`<tr>
        <td>${m.service}.${m.method}</td>
        <td>${t.target}</td>
        <td class="num">${r ? fmt(r.qps) : '-'}</td>
        <td class="num">${fmt(m.count)}</td>
        <td class="num" ${errPct > 1 ? 'style="color:var(--err)"' : ''}>${fmt(errPct, 2)}%</td>
        <td class="num">${fmt(m.avgMs, 2)}</td>
        <td class="num">${fmt(m.p50Ms, 2)}</td>
        <td class="num">${fmt(m.p90Ms, 2)}</td>
        <td class="num">${fmt(m.p99Ms, 2)}</td>
      </tr>`);
    }
  }
  if (!rows.length) {
    $('methods').innerHTML = '<div class="empty">还没有调用记录 —— 跑一下 cmd/client 或压测程序就会出现</div>';
    return;
  }
  $('methods').innerHTML = `<table><thead><tr>
      <th>方法</th><th>实例</th><th class="num">QPS</th><th class="num">累计</th>
      <th class="num">错误率</th><th class="num">平均(ms)</th>
      <th class="num">P50</th><th class="num">P90</th><th class="num">P99</th>
    </tr></thead><tbody>${rows.join('')}</tbody></table>`;
}

// 折线图: 不引图表库, 直接用 canvas 画, 免得为一条线拖个依赖
const CHART_HEIGHT = 140; // 逻辑高度(CSS 像素)

function drawChart(history) {
  const canvas = $('qpsChart');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 600;
  const h = CHART_HEIGHT;

  // 注意: 这里必须用固定的逻辑高度, 不能读 canvas.height ——
  // canvas.height 会被下面写成 h*dpr, 再读就会每次刷新翻一倍
  canvas.style.height = h + 'px';
  canvas.width = Math.round(w * dpr);
  canvas.height = Math.round(h * dpr);

  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);

  const style = getComputedStyle(document.documentElement);
  const line = style.getPropertyValue('--accent').trim() || '#4f9cf9';
  const grid = style.getPropertyValue('--line').trim() || '#2a3348';
  const muted = style.getPropertyValue('--muted').trim() || '#8b97b1';

  const pad = { l: 52, r: 10, t: 10, b: 18 };
  const cw = w - pad.l - pad.r, ch = h - pad.t - pad.b;
  const max = Math.max(10, ...history);

  ctx.strokeStyle = grid; ctx.fillStyle = muted;
  ctx.lineWidth = 1; ctx.font = '11px ui-monospace, monospace';
  for (let i = 0; i <= 3; i++) {
    const y = pad.t + (ch * i) / 3;
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(w - pad.r, y); ctx.stroke();
    const v = max * (1 - i / 3);
    ctx.fillText(v >= 1000 ? `${Math.round(v / 1000)}k` : String(Math.round(v)), 6, y + 4);
  }

  if (history.length < 2) {
    ctx.fillStyle = muted;
    ctx.fillText('采集中…', pad.l + 8, pad.t + ch / 2);
    return;
  }

  const stepX = cw / (state.maxPoints - 1);
  const xy = (i, v) => [pad.l + i * stepX, pad.t + ch - (v / max) * ch];

  // 面积
  ctx.beginPath();
  ctx.moveTo(...xy(0, history[0]));
  history.forEach((v, i) => ctx.lineTo(...xy(i, v)));
  const last = xy(history.length - 1, history[history.length - 1]);
  ctx.lineTo(last[0], pad.t + ch);
  ctx.lineTo(pad.l, pad.t + ch);
  ctx.closePath();
  const grad = ctx.createLinearGradient(0, pad.t, 0, pad.t + ch);
  grad.addColorStop(0, 'rgba(79,156,249,.35)');
  grad.addColorStop(1, 'rgba(79,156,249,0)');
  ctx.fillStyle = grad; ctx.fill();

  // 折线
  ctx.beginPath();
  history.forEach((v, i) => (i === 0 ? ctx.moveTo(...xy(i, v)) : ctx.lineTo(...xy(i, v))));
  ctx.strokeStyle = line; ctx.lineWidth = 2; ctx.stroke();
}

function setStatus(text, cls) {
  const el = $('status');
  el.textContent = text;
  el.className = `status ${cls || ''}`;
}

async function tick() {
  try {
    const res = await fetch('/api/overview', { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    const { rates, totalQPS, totalErrRate } = computeRates(data, state.prev);
    state.prev = data;

    if (state.qpsHistory.length >= state.maxPoints) state.qpsHistory.shift();
    state.qpsHistory.push(totalQPS);

    renderSummary(data, totalQPS, totalErrRate);
    renderTopology(data.topology);
    renderTargets(data.targets);
    renderMethods(data, rates);
    drawChart(state.qpsHistory);

    setStatus(`已更新 ${new Date().toLocaleTimeString('zh-CN')}`, 'ok');
  } catch (e) {
    setStatus(`获取失败: ${e.message}`, 'err');
  }
}

function restartTimer() {
  if (state.timer) clearInterval(state.timer);
  if (state.running) {
    tick();
    state.timer = setInterval(tick, state.intervalMs);
  }
}

$('interval').addEventListener('change', (e) => {
  state.intervalMs = Number(e.target.value);
  restartTimer();
});

$('toggle').addEventListener('click', () => {
  state.running = !state.running;
  $('toggle').textContent = state.running ? '暂停' : '继续';
  if (!state.running) {
    clearInterval(state.timer);
    setStatus('已暂停', '');
  } else {
    restartTimer();
  }
});

window.addEventListener('resize', () => drawChart(state.qpsHistory));

restartTimer();
