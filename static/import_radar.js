"use strict";
const form = document.querySelector('#search-form');
const statusBox = document.querySelector('#status');
const submitButton = document.querySelector('#submit');
const base = '/bil/import-radar/api/search';
let currentReport = null;
let polling = false;
const money = value => value == null ? 'Mangler data' : new Intl.NumberFormat('nb-NO', {maximumFractionDigits:0}).format(value) + ' kr';
function el(tag, text, className) { const n=document.createElement(tag); if(text!=null)n.textContent=text; if(className)n.className=className; return n; }
function setStatus(text, kind='') {statusBox.textContent=text;statusBox.className=kind;}
function quote(r) {return r.purchase_observation?.unconfirmed_net_scenario?.plus_freight_nok ?? r.purchase_observation?.gross_plus_freight_nok;}
function calc(r) {return r.net_scenario_calculation || r.calculation || {};}
function safeLink(url) {try {const u=new URL(url); return u.protocol==='https:' && ['suchen.mobile.de','www.bytbil.com','bytbil.com'].includes(u.hostname) ? u.href : null;}catch{return null;}}
function showCars() {
  const target=document.querySelector('#cars');target.replaceChildren();
  const cars=[...(currentReport?.results || [])];
  if(document.querySelector('#sort').value==='margin') cars.sort((a,b)=>(calc(b).margin_nok ?? -Infinity)-(calc(a).margin_nok ?? -Infinity));
  else cars.sort((a,b)=>(quote(a) ?? Infinity)-(quote(b) ?? Infinity));
  if(!cars.length) {target.append(el('p','Ingen biler med bekreftede søkefelter ble hentet. Se status for hvert nettsted over.','empty'));return;}
  for(const r of cars) {
    const c=calc(r), q=r.purchase_observation || {}, card=el('article',null,'car');
    const head=el('div',null,'car-head'), ident=el('div');
    ident.append(el('span',r.source==='mobile_de'?'TYSKLAND · MOBILE.DE':'SVERIGE · BYTBIL','tag'));
    const h=el('h3'), url=safeLink(r.url), a=el(url?'a':'span',r.variant_text || `${r.make} ${r.model}`);
    if(url){a.href=url;a.target='_blank';a.rel='noopener noreferrer';}h.append(a);ident.append(h);
    ident.append(el('div',`${r.model_year} · ${new Intl.NumberFormat('nb-NO').format(r.mileage_km)} km · ${r.battery_kwh || '?'} kWh · ${r.drive || '?'}`,'hint'));
    head.append(ident,el('span',r.calculation?'Må kontrolleres':'Ufullstendig kalkyle','pill'));card.append(head);
    const metrics=el('div',null,'metrics');
    const net=Boolean(r.net_scenario_calculation);
    for(const [label,value,isMargin] of [
      [q.unconfirmed_net_scenario?'Nettoinnkjøp + frakt*':'Bruttoinnkjøp + frakt',quote(r),false],
      ['Norsk hurtigpris',r.valuation?.hurtigpris,false],
      [net?'Kundepris for marginmålet*':'Kundepris for marginmålet',c.required_customer_price_nok,false],
      [net?'Margin i nettoscenario*':'Margin med bruttoinnkjøp',c.margin_nok,true]]) {
      const metric=el('div',null,'metric'+(isMargin&&value<0?' negative':''));metric.append(el('span',label),el('strong',money(value)));metrics.append(metric);
    }card.append(metrics);
    if(q.unconfirmed_net_scenario) card.append(el('p','* Forutsetter kjøp til annonsert nettopris. Eksportvilkårene er ikke bekreftet.','hint'));
    const detail=el('details');detail.append(el('summary','Se kostnader og kontrollpunkter'));
    const breakdown=el('div',null,'breakdown');
    for(const [label,value] of [['Bruttoinnkjøp + frakt',q.gross_plus_freight_nok],['Frakt',q.freight_nok],['Vektavgift',c.weight_tax_nok],['Vrakpant',c.scrappage_tax_nok],['Øvrige kostnader',c.other_costs_nok],['Reserve',c.reserve_nok],['Moms ved modellens kundepris',c.output_vat_nok],['Markedspris fra modellen',r.valuation?.forventet_pris]]) {
      const row=el('div');row.append(el('span',label),el('strong',money(value)));breakdown.append(row);
    }detail.append(breakdown);
    const ul=el('ul');for(const reason of r.review_reasons || [])ul.append(el('li',reason));detail.append(ul);card.append(detail);target.append(card);
  }
}
function render(report,id) {
  currentReport=report;document.querySelector('#results').hidden=false;
  const fx=report.fx || {};document.querySelector('#fx-note').textContent=`${report.unique_count} biler · ${fx.kind || 'Valuta'} ${fx.date || ''} · EUR/NOK ${Number(fx.eur_nok).toFixed(4)} · SEK/NOK ${Number(fx.sek_nok).toFixed(4)} · Søk ${new Date(report.started_at).toLocaleString('nb-NO')}`;
  document.querySelector('#download').href=`${base}/${encodeURIComponent(id)}/download`;
  const sources=document.querySelector('#sources');sources.replaceChildren();
  for(const s of report.sources || []) {
    const box=el('div',null,'source'+(s.status!=='ok'?' problem':''));
    box.append(el('strong',s.source==='mobile_de'?'Mobile.de · Tyskland':'Bytbil · Sverige'));
    box.append(el('p',`${s.matched || 0} treff · ${s.details_checked || 0} detaljannonser kontrollert${s.status==='error'?' · Kilden kunne ikke fullføres':s.status==='partial'?' · Delvis resultat':''}`));
    for(const error of s.errors || [])box.append(el('p',error));
    const u=safeLink(s.url);if(u){const a=el('a','Åpne søket hos kilden');a.href=u;a.target='_blank';a.rel='noopener noreferrer';box.append(a);}sources.append(box);
  }showCars();
}
async function poll(id) {
  if(polling)return;polling=true;submitButton.disabled=true;
  try {
    for(let attempt=0;attempt<85;attempt++) {
      const res=await fetch(`${base}/${encodeURIComponent(id)}`,{cache:'no-store'});
      if(res.redirected)throw new Error('Du må logge inn igjen. Last siden på nytt.');
      if(!res.ok)throw new Error(res.status===404?'Søket har utløpt. Start et nytt søk.':'Kunne ikke hente søkestatus. Last siden på nytt.');
      const data=await res.json();
      if(data.status==='done'){render(data.report,id);setStatus('Søket er ferdig. Kontroller kildestatus og forutsetninger før du vurderer et kjøp.');return;}
      if(data.status==='error')throw new Error(data.error || 'Søket ble avbrutt');
      setStatus('Henter annonser fra begge land og sammenligner med prismodellen …','busy');
      await new Promise(resolve=>setTimeout(resolve,2500));
    }throw new Error('Søket tar lengre tid enn forventet. Last siden på nytt for å hente status.');
  }catch(error){setStatus(error.message,'error');}
  finally{polling=false;submitButton.disabled=false;}
}
form.addEventListener('submit',async event=>{
  event.preventDefault();if(polling)return;
  const data=Object.fromEntries(new FormData(form));data.vat_only=form.elements.vat_only.checked;
  submitButton.disabled=true;document.querySelector('#results').hidden=true;setStatus('Starter søket …','busy');
  try {
    const res=await fetch(base,{method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':form.dataset.csrf},body:JSON.stringify(data)});
    if(res.redirected)throw new Error('Du må logge inn igjen. Last siden på nytt.');
    const body=await res.json();if(!res.ok)throw new Error(body.error || 'Kunne ikke starte søket');
    try{sessionStorage.setItem('import-radar-job',body.id);localStorage.setItem('import-radar-form',JSON.stringify(data));}catch{}
    await poll(body.id);
  }catch(error){setStatus(error.message,'error');}finally{submitButton.disabled=false;}
});
document.querySelector('#sort').addEventListener('change',showCars);
try {
  const saved=JSON.parse(localStorage.getItem('import-radar-form') || 'null');
  if(saved)for(const [k,v]of Object.entries(saved)){const field=form.elements.namedItem(k);if(!field||k==='registration_date')continue;if(field.type==='checkbox')field.checked=Boolean(v);else field.value=v;}
  const id=sessionStorage.getItem('import-radar-job');if(id)poll(id);
}catch{}
