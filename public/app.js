/* stock-themes.com Clone — App Logic v3 */
const STATE = { themes:[], etfs:[], stocks:[], sparklines:{}, allPeriods:[], activePeriod:"1日",
  showChart:true, showTickers:false, mapCat:"sector", sortBy:"desc", displayMode:"top-bottom" };
const PL = {"日中":"日中","1日":"1D","5日":"5D","10日":"10D","1ヶ月":"1M","2ヶ月":"2M","3ヶ月":"3M","半年":"6M","1年":"1Y"};
const BP = ["5日","10日","1ヶ月","2ヶ月","3ヶ月","半年","1年"];
const MINI_MAP = {
  sector:{label:"セクター",items:["XLB","XLC","XLE","XLF","XLI","XLK","XLP","XLRE","XLU","XLV","XLY"]},
  style:{label:"スタイル",items:["IJH","IJJ","IJK","IVE","IVW","IWM","IWN","IWO","QQQ","RSP","SPY"]},
  commodity:{label:"商品",items:["CPER","GLD","SLV","UNG","USO"]},
  bond:{label:"金利",items:["HYG","IEF","IEI","MBB","SHV","SHY","TLH","TLT"]}
};
const ETF_NAMES={XLB:"素材",XLC:"通信",XLE:"エネルギー",XLF:"金融",XLI:"資本財",XLK:"テクノロジー",XLP:"生活必需品",XLRE:"不動産",XLU:"公益",XLV:"ヘルスケア",XLY:"一般消費財",SPY:"S&P500",QQQ:"NASDAQ100",IWM:"小型株",RSP:"均等加重",IJH:"中型",IJJ:"中型V",IJK:"中型G",IVE:"大型V",IVW:"大型G",IWN:"小型V",IWO:"小型G",GLD:"金",SLV:"銀",USO:"原油",UNG:"天然ガス",CPER:"銅",TLT:"長期国債",TLH:"中長期",IEF:"中期",IEI:"中短期",SHY:"短期",SHV:"超短期",HYG:"ハイイールド",MBB:"MBS"};
function tileColor(v){if(v==null)return"#888";const a=Math.min(Math.abs(v*100)/5,1);if(v>=0){const r=Math.round(129+(34-129)*a),g=Math.round(199+(197-199)*a),b=Math.round(132+(94-132)*a);return`rgb(${r},${g},${b})`}else{const r=Math.round(237+(239-237)*a),g=Math.round(129+(68-129)*a),b=Math.round(134+(68-134)*a);return`rgb(${r},${g},${b})`}}
const fmtRet = v => { if(v==null) return "—"; const p=(v*100).toFixed(1); return v>=0?`+${p}%`:`${p}%`; };

document.addEventListener("DOMContentLoaded", async () => {
  try { await loadData(); bindEvents(); render(); registerSW(); }
  catch(e) { document.getElementById("loading").innerHTML = `エラー<br><small>${e.message}</small>`; }
});

async function registerSW() {
  if ("serviceWorker" in navigator) navigator.serviceWorker.register("/sw.js").catch(()=>{});
}

async function loadData() {
  const [rk, sp] = await Promise.all([
    fetch("/api/theme_ranking.json").then(r=>r.json()),
    fetch("/api/sparklines.json").then(r=>r.json()).catch(()=>({}))
  ]);
  STATE.allPeriods = (rk.all_periods||rk.periods||[]).filter(p=>p!=="日中");
  STATE.activePeriod = STATE.allPeriods.includes("1日")?"1日":STATE.allPeriods[0];
  const all = rk.all_themes||[];
  STATE.themes = all.filter(t=>t.related);
  STATE.etfs = all.filter(t=>t.isETF);
  STATE.stocks = all.filter(t=>t.isIndividualTicker);
  STATE.sparklines = sp;
  sortThemes();
  document.getElementById("headerMeta").textContent = `終値：${rk.latest_stock_date||"—"} | ${STATE.themes.length} テーマ | ${rk.data_source||"yfinance"}`;
}

function sortThemes() {
  const p = STATE.activePeriod;
  if(STATE.sortBy==="desc") STATE.themes.sort((a,b)=>(b[p]??-Infinity)-(a[p]??-Infinity));
  else if(STATE.sortBy==="asc") STATE.themes.sort((a,b)=>(a[p]??Infinity)-(b[p]??Infinity));
  else if(STATE.sortBy==="name") STATE.themes.sort((a,b)=>a.name.localeCompare(b.name,"ja"));
  else STATE.themes.sort((a,b)=>(b[p]??-Infinity)-(a[p]??-Infinity));
}

function bindEvents() {
  document.getElementById("chartToggle").addEventListener("click",function(){STATE.showChart=!STATE.showChart;this.classList.toggle("active",STATE.showChart);renderList();});
  document.getElementById("tickerToggle").addEventListener("click",function(){STATE.showTickers=!STATE.showTickers;this.classList.toggle("active",STATE.showTickers);renderList();});
  document.getElementById("sortSelect").addEventListener("change",function(){STATE.sortBy=this.value;sortThemes();renderList();});
  // Radio → ミニマップ切替（ランキングには影響しない）
  document.querySelectorAll('input[name="cat"]').forEach(r=>r.addEventListener("change",function(){STATE.mapCat=this.value||"sector";renderMiniMap();}));
}

function render() { renderPeriodBar(); renderList(); renderMiniMap(); }

function renderPeriodBar() {
  const bar = document.getElementById("periodBar");
  bar.innerHTML = STATE.allPeriods.map(p=>`<button class="period-btn${p===STATE.activePeriod?" active":""}" data-p="${p}">${PL[p]||p}</button>`).join("");
  bar.onclick = e => { const b=e.target.closest(".period-btn"); if(!b)return; STATE.activePeriod=b.dataset.p; sortThemes(); renderPeriodBar(); renderList(); renderMiniMap(); };
}

function renderList() {
  const main = document.getElementById("main");
  let items = [...STATE.themes];
  const p = STATE.activePeriod;
  const periodLabel = PL[p]||p;

  // ソート: 全モード共通で sortBy を尊重
  if (STATE.sortBy==="desc") items.sort((a,b)=>(b[p]??-Infinity)-(a[p]??-Infinity));
  else if (STATE.sortBy==="asc") items.sort((a,b)=>(a[p]??Infinity)-(b[p]??Infinity));
  else if (STATE.sortBy==="name") items.sort((a,b)=>a.name.localeCompare(b.name,"ja"));
  else items.sort((a,b)=>(b[p]??-Infinity)-(a[p]??-Infinity)); // デフォルト = 暴騰率↑

  // セクションラベル
  const sortLabels = {desc:`${periodLabel} 暴騰率↑`, asc:`${periodLabel} 暴落率↓`, name:"名前順"};
  const sortLabel = sortLabels[STATE.sortBy]||periodLabel;
  const topLabel = `${periodLabel} 暴騰率 TOP 5`;
  const botLabel = `${periodLabel} 暴落率 BOTTOM 5`;

  let html = "";
  if (STATE.displayMode==="top-bottom") {
    const top5 = items.slice(0,5);
    const bot5 = items.slice(-5);
    html += `<div class="section-label top">${topLabel}</div>`;
    html += top5.map((t,i)=>renderItem(t,i+1)).join("");
    // S&P500 benchmark (#10 same style as regular cards)
    const spy = STATE.etfs.find(e=>e.name==="SPY");
    if (spy) {
      const sr = spy[p]; const sd = (sr||0)>=0?"up":"down";
      html += `<div class="theme-item ${sd}"><div class="theme-item-row"><div class="rank ${sd}">—</div><div class="theme-name"><div class="theme-name-primary">S&P500</div><div class="theme-name-secondary">SPY</div></div><div class="return-cell"><div class="return-value ${sd==="up"?"positive":"negative"}">${fmtRet(sr)}</div><div class="return-sub">${fmtRet(spy["1年"])} /1Y</div></div><div class="chevron"></div></div></div>`;
    }
    html += `<div class="section-label bottom">${botLabel}</div>`;
    html += bot5.map((t,i)=>renderItem(t,items.length-4+i)).join("");
  } else {
    const limit = STATE.displayMode==="all"?items.length:STATE.displayMode==="top20"?20:10;
    html += items.slice(0,limit).map((t,i)=>renderItem(t,i+1)).join("");
  }
  // #11 show more buttons with 上下10 as default
  html += `<div class="show-more">
    <button class="show-more-btn${STATE.displayMode==="top-bottom"?" active":""}" onclick="setDisplay('top-bottom')">上下5テーマ</button>
    <button class="show-more-btn${STATE.displayMode==="top20"?" active":""}" onclick="setDisplay('top20')">上下20テーマ</button>
    <button class="show-more-btn${STATE.displayMode==="all"?" active":""}" onclick="setDisplay('all')">全テーマ表示</button>
  </div>`;
  html += '<div class="footer-attr">出所：<a href="#">yfinance</a></div>';
  main.innerHTML = html;
  if (STATE.showChart) {
    requestAnimationFrame(()=>{
      main.querySelectorAll("canvas[data-slug]").forEach(c=>{
        const sd = STATE.sparklines[c.dataset.slug];
        if(sd) drawSparkline(c, sd);
      });
    });
  }
}

function renderItem(theme, displayRank) {
  const p = STATE.activePeriod;
  const ret = theme[p];
  const dir = ret>0?"up":ret<0?"down":"";
  const rankClass = displayRank<=3?"top3":"";
  const yearRet = theme["1年"];
  const sparkHtml = STATE.showChart?`<div class="sparkline-wrap"><canvas data-slug="${theme.slug}" height="52"></canvas></div>`:"";
  // #5 Period breakdown INLINE by default
  const bdHtml = `<div class="period-breakdown">${BP.map(bp=>{
    const v=theme[bp]; if(v===null||v===undefined)return "";
    const cls=v>=0?"positive":"negative";
    return `<div class="pb-item"><div class="pb-label">${bp}</div><div class="pb-val ${cls}">${fmtRet(v)}</div></div>`;
  }).join("")}</div>`;
  let tickerHtml = "";
  if (STATE.showTickers) {
    const tp = theme.tickerPerformances||{};
    const tags = Object.entries(tp).map(([tk,d])=>({tk,r:d[p]??null})).filter(t=>t.r!==null).sort((a,b)=>b.r-a.r).slice(0,8);
    tickerHtml = `<div class="ticker-tags">${tags.map(t=>{
      const cls=t.r>=0?"positive":"negative";
      return `<span class="ticker-tag ${cls}">${t.tk} ${fmtRet(t.r)}</span>`;
    }).join("")}</div>`;
  }
  // #14 Navigate to /theme/{slug} instead of modal
  return `<div class="theme-item ${dir}" onclick="location.href='/theme/${theme.slug}'">
    <div class="theme-item-row">
      <div class="rank ${rankClass} ${dir}">${displayRank}</div>
      <div class="theme-name"><div class="theme-name-primary">${theme.name}</div><div class="theme-name-secondary">${theme.theme1||theme.industry||""}</div></div>
      ${sparkHtml}
      <div class="return-cell"><div class="return-value ${dir==="up"?"positive":dir==="down"?"negative":""}">${fmtRet(ret)}</div><div class="return-sub">${yearRet!=null?fmtRet(yearRet)+" /1Y":""}</div></div>
      <div class="chevron">›</div>
    </div>${bdHtml}${tickerHtml}</div>`;
}

function drawSparkline(canvas, data) {
  const ctx=canvas.getContext("2d"); const vals=data.values||[];
  if(vals.length<2)return;
  const last=vals[vals.length-1]; const color=last>=0?"#22c55e":"#ef4444";
  new Chart(ctx,{type:"line",data:{labels:vals.map((_,i)=>i),datasets:[{data:vals,borderColor:color,borderWidth:1.5,fill:{target:"origin",above:color+"12",below:color+"12"},pointRadius:0,tension:0.3}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{enabled:false}},scales:{x:{display:false},y:{display:false}},animation:false}});
}

window.setDisplay = function(mode) { STATE.displayMode=mode; renderList(); };

function renderMiniMap() {
  const el = document.getElementById("miniMap");
  if (!el) return;
  const p = STATE.activePeriod;
  const cat = MINI_MAP[STATE.mapCat];
  if (!cat) return;
  const tiles = cat.items.map(tk => {
    const e = STATE.etfs.find(x=>x.name===tk);
    return {tk, name:ETF_NAMES[tk]||tk, ret:e?e[p]:null};
  }).sort((a,b)=>(b.ret||0)-(a.ret||0));
  el.innerHTML = '<div class="map-viewport">' + tiles.map(t =>
    `<div class="map-tile" style="background:${tileColor(t.ret)}"><div class="map-tile-name">${t.name}</div><div class="map-tile-change">${fmtRet(t.ret)}</div><div style="font-size:9px;color:rgba(255,255,255,0.7);margin-top:1px">${t.tk}</div></div>`
  ).join('') + '</div>';
}
