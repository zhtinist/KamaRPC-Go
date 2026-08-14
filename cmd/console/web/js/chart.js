// 图表层: 用 canvas 手画折线, 不引图表库 —— 为一条线拖个依赖不划算,
// 也守住"前端零外部依赖"这条线。

import { compact } from './format.js';

/** 逻辑高度(CSS 像素)。必须是常量, 见下面 resize 的注释 */
const HEIGHT = 140;
const PAD = { left: 52, right: 12, top: 12, bottom: 20 };

function cssVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

/**
 * 按设备像素比重设画布尺寸。
 *
 * 坑: 不能读 canvas.height 当逻辑高度 —— 它下面会被写成 h*dpr, 下一轮再读
 * 就是放大后的值, 于是每刷新一次高度翻一倍, 几秒后画布大到渲染失败。
 * 逻辑尺寸只能来自常量或 CSS 尺寸。
 */
function resize(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const width = canvas.clientWidth || 600;

  canvas.style.height = `${HEIGHT}px`;
  canvas.width = Math.round(width * dpr);
  canvas.height = Math.round(HEIGHT * dpr);

  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0); // 重设而不是累乘, 避免多次调用后缩放叠加
  return { ctx, width, height: HEIGHT };
}

export function drawSeries(canvas, history, { maxPoints }) {
  if (!canvas) return;

  const { ctx, width, height } = resize(canvas);
  ctx.clearRect(0, 0, width, height);

  const colors = {
    line: cssVar('--accent', '#4f9cf9'),
    grid: cssVar('--line', '#2a3348'),
    muted: cssVar('--muted', '#8b97b1'),
  };

  const plotW = width - PAD.left - PAD.right;
  const plotH = height - PAD.top - PAD.bottom;
  const max = Math.max(10, ...history);

  // 网格与刻度
  ctx.strokeStyle = colors.grid;
  ctx.fillStyle = colors.muted;
  ctx.lineWidth = 1;
  ctx.font = '11px ui-monospace, SFMono-Regular, Menlo, monospace';
  for (let i = 0; i <= 3; i++) {
    const y = PAD.top + (plotH * i) / 3;
    ctx.beginPath();
    ctx.moveTo(PAD.left, y);
    ctx.lineTo(width - PAD.right, y);
    ctx.stroke();
    ctx.fillText(compact(max * (1 - i / 3)), 8, y + 4);
  }

  if (history.length < 2) {
    ctx.fillStyle = colors.muted;
    ctx.fillText('采集中…（速率需要两次采样才能算出）', PAD.left + 8, PAD.top + plotH / 2);
    return;
  }

  const stepX = plotW / (maxPoints - 1);
  const pointAt = (i, v) => [PAD.left + i * stepX, PAD.top + plotH - (v / max) * plotH];

  // 面积
  ctx.beginPath();
  ctx.moveTo(...pointAt(0, history[0]));
  history.forEach((v, i) => ctx.lineTo(...pointAt(i, v)));
  const [lastX] = pointAt(history.length - 1, history.at(-1));
  ctx.lineTo(lastX, PAD.top + plotH);
  ctx.lineTo(PAD.left, PAD.top + plotH);
  ctx.closePath();

  const grad = ctx.createLinearGradient(0, PAD.top, 0, PAD.top + plotH);
  grad.addColorStop(0, 'rgba(79, 156, 249, 0.35)');
  grad.addColorStop(1, 'rgba(79, 156, 249, 0)');
  ctx.fillStyle = grad;
  ctx.fill();

  // 折线
  ctx.beginPath();
  history.forEach((v, i) => (i === 0 ? ctx.moveTo(...pointAt(i, v)) : ctx.lineTo(...pointAt(i, v))));
  ctx.strokeStyle = colors.line;
  ctx.lineWidth = 2;
  ctx.stroke();

  // 当前点
  const [cx, cy] = pointAt(history.length - 1, history.at(-1));
  ctx.beginPath();
  ctx.arc(cx, cy, 3, 0, Math.PI * 2);
  ctx.fillStyle = colors.line;
  ctx.fill();
}
