/* stock-themes.com Clone — App Logic v2 */
const STATE = { themes:[], etfs:[], stocks:[], sparklines:{}, allPeriods:[], activePeriod:"1日",
  showChart:true, showTickers:false, catFilter:"", sortBy:"rank", displayMode:"top-bottom" };
const PL = {"日中":"日中","1日":"1D","5日":"5D","10日":"10D","1ヶ月":"1M","2ヶ月":"2M","3ヶ月":"3M","半年":"6M","1年":"1Y"};
const BP = ["5日","10日","1ヶ月","2ヶ月","3ヶ月","半年","1年"];
const CAT_MAP = {
  sector: ["テクノロジー","ヘルスケア","金融","エネルギー","消費者一般","消費者必需品","資本財","素材","不動産","公益","通信","その他"],
  style: [], commodity: ["エネルギー","素材"], bond: ["金融"]
};
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
  else STATE.themes.sort((a,b)=>(a.rank??999)-(b.rank??999));
}

function bindEvents() {
  document.getElementById("chartToggle").addEventListener("click",function(){STATE.showChart=!STATE.showChart;this.classList.toggle("active",STATE.showChart);renderList();});
  document.getElementById("tickerToggle").addEventListener("click",function(){STATE.showTickers=!STATE.showTickers;this.classList.toggle("active",STATE.showTickers);renderList();});
  document.getElementById("sortSelect").addEventListener("change",function(){STATE.sortBy=this.value;sortThemes();renderList();});
  // #6 Radio filter
  document.querySelectorAll('input[name="cat"]').forEach(r=>r.addEventListener("change",function(){STATE.catFilter=this.value;renderList();}));
  // #7 Market toggle
  document.querySelectorAll(".mt-btn").forEach(b=>b.addEventListener("click",function(){
    document.querySelectorAll(".mt-btn").forEach(x=>x.classList.remove("active"));
    this.classList.add("active");
  }));
}

function render() { renderPeriodBar(); renderList(); }

function renderPeriodBar() {
  const bar = document.getElementById("periodBar");
  bar.innerHTML = STATE.allPeriods.map(p=>`<button class="period-btn${p===STATE.activePeriod?" active":""}" data-p="${p}">${PL[p]||p}</button>`).join("");
  bar.onclick = e => { const b=e.target.closest(".period-btn"); if(!b)return; STATE.activePeriod=b.dataset.p; sortThemes(); renderPeriodBar(); renderList(); };
}

function filterByCat(items) {
  if (!STATE.catFilter) return items;
  if (STATE.catFilter==="sector") return items;
  if (STATE.catFilter==="commodity") return items.filter(t=>["エネルギー","素材"].includes(t.industry));
  if (STATE.catFilter==="bond") return items.filter(t=>t.industry==="金融");
  if (STATE.catFilter==="style") return items.filter(t=>["消費者一般","消費者必需品"].includes(t.industry));
  return items;
}

function renderList() {
  const main = document.getElementById("main");
  let items = filterByCat([...STATE.themes]);
  const p = STATE.activePeriod;
  const periodLabel = PL[p]||p;

  // ソート: 全モード共通で sortBy を尊重
  if (STATE.sortBy==="desc") items.sort((a,b)=>(b[p]??-Infinity)-(a[p]??-Infinity));
  else if (STATE.sortBy==="asc") items.sort((a,b)=>(a[p]??Infinity)-(b[p]??Infinity));
  else if (STATE.sortBy==="name") items.sort((a,b)=>a.name.localeCompare(b.name,"ja"));
  else items.sort((a,b)=>(b[p]??-Infinity)-(a[p]??-Infinity)); // ランク順 = 選択期間の騰落率順

  // セクションラベル
  const sortLabels = {desc:`${periodLabel} 騰落率↑`, asc:`${periodLabel} 騰落率↓`, name:"名前順", rank:`${periodLabel} 騰落率`};
  const sortLabel = sortLabels[STATE.sortBy]||periodLabel;

  let html = "";
  if (STATE.displayMode==="top-bottom") {
    const top10 = items.slice(0,10);
    const bot10 = items.slice(-10);
    html += `<div class="section-label top">${sortLabel} TOP 10</div>`;
    html += top10.map((t,i)=>renderItem(t,i+1)).join("");
    // S&P500 benchmark (#10 same style as regular cards)
    const spy = STATE.etfs.find(e=>e.name==="SPY");
    if (spy) {
      const sr = spy[p]; const sd = (sr||0)>=0?"up":"down";
      html += `<div class="theme-item ${sd}"><div class="theme-item-row"><div class="rank ${sd}">—</div><div class="theme-name"><div class="theme-name-primary">S&P500</div><div class="theme-name-secondary">SPY</div></div><div class="return-cell"><div class="return-value ${sd==="up"?"positive":"negative"}">${fmtRet(sr)}</div><div class="return-sub">${fmtRet(spy["1年"])} /1Y</div></div><div class="chevron"></div></div></div>`;
    }
    html += `<div class="section-label bottom">${sortLabel} BOTTOM 10</div>`;
    html += bot10.map((t,i)=>renderItem(t,items.length-9+i)).join("");
  } else {
    const limit = STATE.displayMode==="all"?items.length:STATE.displayMode==="top20"?20:10;
    html += items.slice(0,limit).map((t,i)=>renderItem(t,i+1)).join("");
  }
  // #11 show more buttons with 上下10 as default
  html += `<div class="show-more">
    <button class="show-more-btn${STATE.displayMode==="top-bottom"?" active":""}" onclick="setDisplay('top-bottom')">上下10テーマ</button>
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
  const last=vals[vals.length-1]; const color=last>=0?"#16a34a":"#dc2626";
  new Chart(ctx,{type:"line",data:{labels:vals.map((_,i)=>i),datasets:[{data:vals,borderColor:color,borderWidth:1.5,fill:{target:"origin",above:color+"12",below:color+"12"},pointRadius:0,tension:0.3}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{enabled:false}},scales:{x:{display:false},y:{display:false}},animation:false}});
}

window.setDisplay = function(mode) { STATE.displayMode=mode; renderList(); };
