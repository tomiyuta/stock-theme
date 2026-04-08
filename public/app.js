/* stock-themes.com Clone — App Logic */
const STATE = { themes:[], etfs:[], stocks:[], sparklines:{}, allPeriods:[], activePeriod:"1日", showChart:true, showTickers:false, showBreakdown:false, filterIndustry:"", sortBy:"rank", displayMode:"top-bottom" };
const PL = {"日中":"日中","1日":"1D","5日":"5D","10日":"10D","1ヶ月":"1M","2ヶ月":"2M","3ヶ月":"3M","半年":"6M","1年":"1Y"};
const BP = ["5日","10日","1ヶ月","2ヶ月","3ヶ月","半年","1年"];

document.addEventListener("DOMContentLoaded", async () => {
  try { await loadData(); bindEvents(); render(); }
  catch(e) { document.getElementById("loading").innerHTML = `エラー<br><small>${e.message}</small>`; }
});

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
  else STATE.themes.sort((a,b)=>(a.rank??999)-(b.rank??999));
}

function bindEvents() {
  document.getElementById("chartToggle").addEventListener("click",function(){STATE.showChart=!STATE.showChart;this.classList.toggle("active",STATE.showChart);renderList();});
  document.getElementById("tickerToggle").addEventListener("click",function(){STATE.showTickers=!STATE.showTickers;this.classList.toggle("active",STATE.showTickers);renderList();});
  document.getElementById("breakdownToggle").addEventListener("click",function(){STATE.showBreakdown=!STATE.showBreakdown;this.classList.toggle("active",STATE.showBreakdown);renderList();});
  document.getElementById("industryFilter").addEventListener("change",function(){STATE.filterIndustry=this.value;renderList();});
  document.getElementById("sortSelect").addEventListener("change",function(){STATE.sortBy=this.value;sortThemes();renderList();});
  document.getElementById("modalClose").addEventListener("click",closeModal);
  document.getElementById("modal").addEventListener("click",e=>{if(e.target.id==="modal")closeModal();});
  document.addEventListener("keydown",e=>{if(e.key==="Escape")closeModal();});
}

function render() { renderPeriodBar(); renderIndustryFilter(); renderList(); }

function renderPeriodBar() {
  const bar = document.getElementById("periodBar");
  bar.innerHTML = STATE.allPeriods.map(p=>`<button class="period-btn${p===STATE.activePeriod?" active":""}" data-p="${p}">${PL[p]||p}</button>`).join("");
  bar.onclick = e => { const b=e.target.closest(".period-btn"); if(!b)return; STATE.activePeriod=b.dataset.p; sortThemes(); renderPeriodBar(); renderList(); };
}

function renderIndustryFilter() {
  const inds = [...new Set(STATE.themes.map(t=>t.industry).filter(Boolean))].sort();
  const sel = document.getElementById("industryFilter");
  sel.innerHTML = `<option value="">全セクター (${STATE.themes.length})</option>` + inds.map(i=>{const n=STATE.themes.filter(t=>t.industry===i).length;return `<option value="${i}">${i} (${n})</option>`;}).join("");
}

function renderList() {
  const main = document.getElementById("main");
  let items = [...STATE.themes];
  if(STATE.filterIndustry) items = items.filter(t=>t.industry===STATE.filterIndustry);
  const p = STATE.activePeriod;
  if(STATE.sortBy!=="name") items.sort((a,b)=>(b[p]??-Infinity)-(a[p]??-Infinity));
  let h = "";
  if(STATE.displayMode==="top-bottom" && !STATE.filterIndustry) {
    h += '<div class="section-label top">TOP 5</div>';
    h += items.slice(0,5).map((t,i)=>renderItem(t,i+1)).join("");
    const spy = STATE.etfs.find(e=>e.name==="SPY");
    if(spy) h += renderBenchmark(spy);
    h += '<div class="section-label bottom">BOTTOM 5</div>';
    h += items.slice(-5).map((t,i)=>renderItem(t,items.length-4+i)).join("");
  } else {
    const lim = STATE.displayMode==="all"?items.length:STATE.displayMode==="top20"?20:10;
    h += items.slice(0,lim).map((t,i)=>renderItem(t,i+1)).join("");
  }
  h += `<div class="show-more">
    <button class="show-more-btn${STATE.displayMode==="top-bottom"?" active":""}" onclick="setDisplay('top-bottom')">上下5テーマ</button>
    <button class="show-more-btn${STATE.displayMode==="top20"?" active":""}" onclick="setDisplay('top20')">上下20テーマ</button>
    <button class="show-more-btn${STATE.displayMode==="all"?" active":""}" onclick="setDisplay('all')">全テーマ表示</button>
  </div>`;
  h += '<div class="footer-attr">出所：yfinance | Self-Hosted Clone</div>';
  main.innerHTML = h;
  if(STATE.showChart) requestAnimationFrame(()=>{main.querySelectorAll("canvas[data-slug]").forEach(c=>{const sd=STATE.sparklines[c.dataset.slug];if(sd)drawSparkline(c,sd);});});
}

function renderItem(theme, displayRank) {
  const p=STATE.activePeriod, ret=theme[p], dir=ret>0?"up":ret<0?"dn":"", t3=displayRank<=3?"top3":"", yearRet=theme["1年"];
  const sparkHtml = STATE.showChart?`<div class="sparkline-wrap"><canvas data-slug="${theme.slug}" height="52"></canvas></div>`:"";
  let breakdownHtml = "";
  if(STATE.showBreakdown) {
    breakdownHtml = `<div class="period-breakdown">${BP.map(bp=>{const v=theme[bp];if(v==null)return"";const cls=v>=0?"positive":"negative";return `<div class="pb-item"><div class="pb-label">${bp}</div><div class="pb-val ${cls}">${fmtRet(v)}</div></div>`;}).join("")}</div>`;
  }
  let tickerHtml = "";
  if(STATE.showTickers) {
    const tp=theme.tickerPerformances||{};
    const tags=Object.entries(tp).map(([tk,d])=>({tk,r:d[p]??null})).filter(t=>t.r!==null).sort((a,b)=>b.r-a.r).slice(0,8);
    tickerHtml = `<div class="ticker-tags">${tags.map(t=>{const cls=t.r>=0?"positive":"negative";return `<span class="ticker-tag ${cls}">${t.tk} ${fmtRet(t.r)}</span>`;}).join("")}</div>`;
  }
  return `<div class="theme-item ${dir}" onclick="openDetail('${theme.slug}')">
    <div class="theme-item-row">
      <div class="rank ${t3} ${dir}">${displayRank}</div>
      <div class="theme-name">
        <div class="theme-name-primary">${theme.name}</div>
        <div class="theme-name-secondary">${theme.theme1||theme.industry||""}</div>
      </div>
      ${sparkHtml}
      <div class="return-cell">
        <div class="return-value ${dir==="up"?"positive":dir==="dn"?"negative":""}">${fmtRet(ret)}</div>
        <div class="return-sub">${yearRet!=null?fmtRet(yearRet)+" /1Y":""}</div>
      </div>
      <div class="chevron">›</div>
    </div>${breakdownHtml}${tickerHtml}</div>`;
}

function renderBenchmark(spy) {
  const p=STATE.activePeriod, ret=spy[p], dir=ret>0?"up":"dn", yearRet=spy["1年"];
  return `<div class="theme-item benchmark"><div class="theme-item-row">
    <div class="rank top3 neutral">—</div>
    <div class="theme-name"><div class="theme-name-primary">S&P500</div><div class="theme-name-secondary">SPY (ベンチマーク)</div></div>
    <div class="return-cell"><div class="return-value ${dir==="up"?"positive":"negative"}">${fmtRet(ret)}</div><div class="return-sub">${yearRet!=null?fmtRet(yearRet)+" /1Y":""}</div></div>
    <div class="chevron"></div></div></div>`;
}

function drawSparkline(canvas, data) {
  const ctx=canvas.getContext("2d"), vals=data.values||[];
  if(vals.length<2) return;
  const color = vals[vals.length-1]>=0?"#16a34a":"#dc2626";
  new Chart(ctx, {type:"line", data:{labels:vals.map((_,i)=>i), datasets:[{data:vals, borderColor:color, borderWidth:1.5, fill:{target:"origin",above:color+"12",below:color+"12"}, pointRadius:0, tension:0.3}]}, options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false},tooltip:{enabled:false}}, scales:{x:{display:false},y:{display:false}}, animation:false}});
}

async function openDetail(slug) {
  const modal=document.getElementById("modal"), body=document.getElementById("modalBody");
  modal.classList.add("open");
  body.innerHTML = '<div class="loading">読み込み中...</div>';
  const theme = STATE.themes.find(t=>t.slug===slug);
  if(!theme){body.innerHTML='<div class="loading">Not found</div>';return;}
  let ab = {};
  try { const abAll = await fetch("/api/alpha_beta.json").then(r=>r.json()); ab = (abAll["3ヶ月"]||{})[slug]||{}; } catch(e){}
  const periods = STATE.allPeriods;
  const pgHtml = periods.map(p=>{const v=theme[p];const cls=v>0?"positive":v<0?"negative":"";return `<div class="dp-box"><div class="dp-label">${PL[p]||p}</div><div class="dp-value ${cls}">${fmtRet(v)}</div></div>`;}).join("");
  const p = STATE.activePeriod;
  const tp = theme.tickerPerformances||{};
  const rows = Object.entries(tp).map(([tk,d])=>({tk,r:d[p]??null})).sort((a,b)=>(b.r??-Infinity)-(a.r??-Infinity));
  const tickerCount = theme.related?theme.related.split(",").filter(t=>t.trim()).length:0;
  const rowsHtml = rows.map(t=>{
    const cls=(t.r??0)>=0?"positive":"negative";
    const abData=ab[t.tk];
    const abStr=abData?`<div class="dt-sub">α${abData.alpha>0?"+":""}${abData.alpha} β${abData.beta} R²${abData.r2}</div>`:"";
    return `<div class="dt-row"><div><div class="dt-name">${t.tk}</div>${abStr}</div><div class="dt-return ${cls}">${fmtRet(t.r)}</div></div>`;
  }).join("");
  body.innerHTML = `
    <div class="detail-title">${theme.name}</div>
    <div class="detail-meta">${theme.industry} › ${theme.theme1} › ${theme.theme2} · ${tickerCount}銘柄</div>
    <div class="detail-periods">${pgHtml}</div>
    <div class="detail-section">構成銘柄 (${PL[p]||p})</div>${rowsHtml}`;
}

function closeModal() { document.getElementById("modal").classList.remove("open"); }
window.setDisplay = function(mode) { STATE.displayMode=mode; renderList(); };
window.openDetail = openDetail;
function fmtRet(val) { if(val==null) return "—"; const pct=(val*100).toFixed(1); return val>=0?`+${pct}%`:`${pct}%`; }
