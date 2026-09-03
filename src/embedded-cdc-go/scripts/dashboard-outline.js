#!/usr/bin/env node
/* ---------------------------------------------------------------------------
 * Grafana 대시보드 JSON 을 사람이 읽고 diff 할 수 있는 개요로 뽑는다.
 *
 * 왜 필요한가. 대시보드 JSON 은 3,000줄이 넘고 들여쓰기가 값보다 길다.
 * 패널 하나에 쿼리 한 줄을 더해도 raw diff 에서는 그 변화가 묻힌다.
 * 실제로 이번에 "지연 추이 패널에 시계 편차 시계열 추가" 를 했는데
 * 바뀐 줄은 10줄이고 파일은 3,079줄이었다.
 *
 * 그래서 "무엇을 그리는가" 만 한 줄에 하나씩 뽑는다. 좌표·색·폰트처럼
 * 눈으로 보면 아는 것은 뺀다 — 여기서 잡고 싶은 것은 쿼리와 임계값이다.
 *
 * 사용법
 *   node dashboard-outline.js <대시보드.json>              개요를 표준출력으로
 *   node dashboard-outline.js --write <대시보드.json ...>   .outline.txt 로 저장
 *   node dashboard-outline.js --check <대시보드.json ...>   .outline.txt 가 최신인지
 *   node dashboard-outline.js --diff <이전.json> <이후.json>  두 판을 견줌
 *
 * --write 로 만든 .outline.txt 를 JSON 과 함께 커밋해 두면
 * 그 뒤로는 git diff 만으로 대시보드 변경이 읽힌다.
 * ------------------------------------------------------------------------- */
'use strict';
const fs = require('fs');
const path = require('path');

function read(file) {
  // BOM 이 붙어 있으면 JSON.parse 가 죽는다. Grafana 가 내보낸 파일에 종종 붙는다.
  return JSON.parse(fs.readFileSync(file, 'utf8').replace(/^﻿/, ''));
}

/** 여러 줄 쿼리를 한 줄로. 줄바꿈 위치만 바뀐 것을 변경으로 잡지 않기 위해서다. */
function flat(text) {
  return String(text == null ? '' : text).replace(/\s+/g, ' ').trim();
}

function thresholds(defaults) {
  const steps = defaults && defaults.thresholds && defaults.thresholds.steps;
  if (!Array.isArray(steps) || steps.length === 0) return null;
  return steps.map(s => (s.value === null || s.value === undefined ? 'base' : s.value) + ':' + s.color).join(' ');
}

function panelLines(panel, out, indent) {
  const pad = ' '.repeat(indent);
  const g = panel.gridPos || {};
  const pos = `@ x${g.x || 0} y${g.y || 0} ${g.w || 0}x${g.h || 0}`;
  out.push(`${pad}panel #${panel.id} ${panel.type} "${panel.title || ''}" ${pos}`);

  const d = (panel.fieldConfig && panel.fieldConfig.defaults) || {};
  const fmt = [];
  if (d.unit) fmt.push('unit=' + d.unit);
  if (d.decimals !== undefined) fmt.push('decimals=' + d.decimals);
  if (d.min !== undefined) fmt.push('min=' + d.min);
  if (d.max !== undefined) fmt.push('max=' + d.max);
  if (fmt.length) out.push(`${pad}  ${fmt.join('  ')}`);

  const th = thresholds(d);
  if (th) out.push(`${pad}  thresholds ${th}`);

  const overrides = (panel.fieldConfig && panel.fieldConfig.overrides) || [];
  if (overrides.length) out.push(`${pad}  overrides ${overrides.length}건`);

  const calcs = panel.options && panel.options.reduceOptions && panel.options.reduceOptions.calcs;
  if (Array.isArray(calcs) && calcs.length) out.push(`${pad}  calc ${calcs.join(',')}`);

  for (const t of panel.targets || []) {
    // expr 이 없는 타깃(텍스트 패널 등)은 건너뛴다
    if (!t.expr && !t.rawSql) continue;
    const legend = t.legendFormat ? `  legend="${flat(t.legendFormat)}"` : '';
    const inst = t.instant ? '  instant' : '';
    out.push(`${pad}  ${t.refId || '?'}  ${flat(t.expr || t.rawSql)}${legend}${inst}`);
  }

  // 텍스트 패널의 본문은 내용 자체가 문서다. 첫 줄만 남겨 바뀐 것을 알아챌 수 있게 한다.
  const content = panel.options && panel.options.content;
  if (content) out.push(`${pad}  text: ${flat(content).slice(0, 100)}`);

  if (panel.description) out.push(`${pad}  desc: ${flat(panel.description).slice(0, 160)}`);
}

function outline(dash) {
  const out = [];
  out.push(`# ${dash.title || '(제목 없음)'}`);
  out.push(`uid=${dash.uid || '-'}  schema=${dash.schemaVersion || '-'}  refresh=${dash.refresh || '-'}` +
           `  time=${(dash.time && dash.time.from) || '-'}~${(dash.time && dash.time.to) || '-'}`);
  const tags = (dash.tags || []).join(',');
  if (tags) out.push(`tags=${tags}`);

  for (const v of (dash.templating && dash.templating.list) || []) {
    out.push(`var $${v.name} ${v.type} ${flat(v.query && (v.query.query || v.query))}`);
  }
  out.push('');

  for (const panel of dash.panels || []) {
    if (panel.type === 'row') {
      out.push(`[row] ${panel.title || ''}${panel.collapsed ? ' (접힘)' : ''}`);
      // 접힌 행은 자식 패널을 자기 안에 담는다. 펼친 행은 형제로 늘어놓는다.
      for (const child of panel.panels || []) panelLines(child, out, 2);
      continue;
    }
    panelLines(panel, out, 2);
  }
  return out.join('\n') + '\n';
}

/** 두 개요를 줄 단위로 견준다. 외부 diff 도구 없이 파일 하나로 끝내기 위해 직접 짠다. */
function diff(a, b) {
  const A = a.split('\n');
  const B = b.split('\n');
  // 최장 공통 부분수열. 대시보드 개요는 길어야 수백 줄이라 O(n*m) 으로 충분하다.
  const n = A.length, m = B.length;
  const lcs = Array.from({ length: n + 1 }, () => new Uint32Array(m + 1));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      lcs[i][j] = A[i] === B[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
    }
  }
  const out = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (A[i] === B[j]) { out.push('  ' + A[i]); i++; j++; }
    else if (lcs[i + 1][j] >= lcs[i][j + 1]) { out.push('- ' + A[i]); i++; }
    else { out.push('+ ' + B[j]); j++; }
  }
  while (i < n) out.push('- ' + A[i++]);
  while (j < m) out.push('+ ' + B[j++]);

  // 바뀐 줄만 추리되 앞뒤 3줄을 남긴다. 어느 패널의 변화인지 알아야 하기 때문이다.
  const changed = out.map(l => l[0] !== ' ');
  const keep = new Set();
  changed.forEach((c, k) => { if (c) for (let d = -3; d <= 3; d++) keep.add(k + d); });
  if (!changed.some(Boolean)) return null;

  const lines = [];
  let gap = false;
  out.forEach((l, k) => {
    if (keep.has(k)) { lines.push(l); gap = false; }
    else if (!gap) { lines.push('  ...'); gap = true; }
  });
  return lines.join('\n');
}

function main(argv) {
  const mode = argv[0] && argv[0].startsWith('--') ? argv.shift() : null;

  if (mode === '--diff') {
    if (argv.length !== 2) { console.error('사용법: --diff <이전.json> <이후.json>'); process.exit(2); }
    const d = diff(outline(read(argv[0])), outline(read(argv[1])));
    if (d === null) { console.log('대시보드 개요에 차이가 없다 (좌표·색만 바뀌었을 수 있다)'); return; }
    console.log(`--- ${argv[0]}\n+++ ${argv[1]}\n${d}`);
    process.exitCode = 1; // 차이가 있으면 1 — CI 에서 검토 강제용으로 쓸 수 있다
    return;
  }

  // 사이드카가 JSON 보다 뒤처졌는지 본다. 뒤처진 개요는 없는 것보다 나쁘다 —
  // git diff 가 "변경 없음" 이라고 거짓말하게 된다. pre-commit 이나 CI 에서 쓴다.
  if (mode === '--check') {
    let stale = 0;
    for (const file of argv) {
      const target = file.replace(/\.json$/, '') + '.outline.txt';
      const fresh = outline(read(file));
      const saved = fs.existsSync(target) ? fs.readFileSync(target, 'utf8') : null;
      if (saved === null) { console.error('없음:   ' + path.basename(target)); stale++; }
      else if (saved !== fresh) { console.error('뒤처짐: ' + path.basename(target)); stale++; }
      else console.log('최신:   ' + path.basename(target));
    }
    if (stale) {
      console.error('\n--write 로 다시 만들 것: node scripts/dashboard-outline.js --write <경로>');
      process.exitCode = 1;
    }
    return;
  }

  if (argv.length === 0) {
    console.error('사용법: node dashboard-outline.js [--write|--check|--diff] <대시보드.json ...>');
    process.exit(2);
  }

  for (const file of argv) {
    const text = outline(read(file));
    if (mode === '--write') {
      const target = file.replace(/\.json$/, '') + '.outline.txt';
      fs.writeFileSync(target, text, 'utf8');
      console.log('기록: ' + path.basename(target) + '  (' + text.split('\n').length + '줄)');
    } else {
      if (argv.length > 1) console.log('===== ' + path.basename(file) + ' =====');
      process.stdout.write(text);
    }
  }
}

main(process.argv.slice(2));
