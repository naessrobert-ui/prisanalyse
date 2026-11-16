// Lokalt møteopptak for podkast og etterarbeid.
// Alt tas opp og lagres i nettleseren til den som trykker «Ta opp». Ingenting lastes opp.
// Gir tre typer filer når opptaket stoppes:
//   1. Video av møtet (alle kameraer/delte skjermer satt sammen) med miksede stemmer.
//   2. Miksede stemmer som egen lydfil.
//   3. Ett lydspor per deltaker («multitrack»), for redigering i podkastverktøy.
window.MeetingRecorder = class {
  // getVisuals(): [{key,name,stream|null,kind:'cam'|'screen'|'none',mirror,speaking}]
  // getAudio(): Map key -> {name, stream}
  constructor({getVisuals,getAudio,onStop,onError}){
    Object.assign(this,{getVisuals,getAudio,onStop,onError});
    this.videos=new Map();this.sources=new Map();this.tracks=new Map();this.recorders=[];this.stopped=false;
    this.W=1280;this.H=720;this.fps=25;
  }
  static pick(list){
    if(!window.MediaRecorder)return null;
    for(const t of list)try{if(MediaRecorder.isTypeSupported(t))return t;}catch(e){}
    return null;
  }
  static supported(){
    return !!(window.MediaRecorder&&HTMLCanvasElement.prototype.captureStream&&(window.AudioContext||window.webkitAudioContext));
  }
  static ext(mime){return /mp4/.test(mime)?(/^audio/.test(mime)?'m4a':'mp4'):/ogg/.test(mime)?'ogg':'webm';}

  async start(){
    if(!MeetingRecorder.supported())throw Error('Denne nettleseren kan ikke ta opp møtet. Bruk oppdatert Chrome, Edge eller Firefox på PC/Mac.');
    this.videoMime=MeetingRecorder.pick(['video/mp4;codecs=avc1.42E01F,mp4a.40.2','video/mp4;codecs=avc1,mp4a','video/webm;codecs=vp9,opus','video/webm;codecs=vp8,opus','video/webm']);
    this.audioMime=MeetingRecorder.pick(['audio/mp4;codecs=mp4a.40.2','audio/webm;codecs=opus','audio/webm','audio/ogg;codecs=opus']);
    if(!this.videoMime||!this.audioMime)throw Error('Nettleseren støtter ikke et filformat for opptak.');
    this.ctx=new (window.AudioContext||window.webkitAudioContext)({sampleRate:48000});
    await this.ctx.resume();
    this.mix=this.ctx.createMediaStreamDestination();
    this.mixGain=this.ctx.createGain();this.mixGain.connect(this.mix);
    // Canvas som tegner møtet. Timer i en Worker, så tegningen fortsetter når fanen er i bakgrunnen.
    this.canvas=document.createElement('canvas');this.canvas.width=this.W;this.canvas.height=this.H;
    this.g=this.canvas.getContext('2d',{alpha:false});
    this.draw();
    const videoStream=this.canvas.captureStream(this.fps);
    const combined=new MediaStream([...videoStream.getVideoTracks(),...this.mix.stream.getAudioTracks()]);
    this.startAt=performance.now();this.startDate=new Date();
    this.recorders.push(this.makeRecorder('video',combined,this.videoMime,{videoBitsPerSecond:3000000,audioBitsPerSecond:160000},'Video av møtet'));
    this.recorders.push(this.makeRecorder('mix',this.mix.stream,this.audioMime,{audioBitsPerSecond:160000},'Alle stemmer (miks)'));
    this.update();
    const code='let t=null;onmessage=e=>{clearInterval(t);if(e.data>0)t=setInterval(()=>postMessage(0),e.data);}';
    this.workerUrl=URL.createObjectURL(new Blob([code],{type:'text/javascript'}));
    this.worker=new Worker(this.workerUrl);
    this.worker.onmessage=()=>{if(!this.stopped)this.draw();};
    this.worker.postMessage(Math.round(1000/this.fps));
    this.syncTimer=setInterval(()=>this.update(),1000);
    this.ctx.onstatechange=()=>{if(!this.stopped&&this.ctx.state==='suspended')this.ctx.resume().catch(()=>{});};
  }

  makeRecorder(kind,stream,mime,opts,label,offset=0){
    const rec=new MediaRecorder(stream,{mimeType:mime,...opts});
    const entry={kind,rec,mime,label,offset,chunks:[],done:null};
    entry.done=new Promise(resolve=>{rec.onstop=resolve;});
    rec.ondataavailable=e=>{if(e.data&&e.data.size)entry.chunks.push(e.data);};
    rec.onerror=e=>{if(!this.stopped)this.onError?.('Opptaket stoppet uventet: '+(e.error?.message||'ukjent feil'));};
    rec.start(5000);
    return entry;
  }

  elapsed(){return this.startAt?(performance.now()-this.startAt)/1000:0;}

  // Hold lydmiks, deltakerspor og videoelementer i takt med hvem som er i møtet.
  update(){
    if(this.stopped||!this.ctx)return;
    const audio=this.getAudio();
    for(const [key,entry] of this.sources){
      const now=audio.get(key)?.stream?.getAudioTracks()[0];
      if(now!==entry.track){entry.node.disconnect();this.sources.delete(key);}
    }
    for(const [key,{name,stream}] of audio){
      const track=stream?.getAudioTracks()[0];
      if(!track||track.readyState!=='live'||this.sources.has(key))continue;
      let node;try{node=this.ctx.createMediaStreamSource(new MediaStream([track]));}catch(e){continue;}
      node.connect(this.mixGain);
      // Eget spor per deltaker. Samme destinasjon gjenbrukes om noen kobler fra og til igjen,
      // så hver person får én sammenhengende fil.
      let t=this.tracks.get(key);
      if(!t){
        const dest=this.ctx.createMediaStreamDestination();
        const offset=this.elapsed();
        t={dest,name,entry:null};this.tracks.set(key,t);
        t.entry=this.makeRecorder('track',dest.stream,this.audioMime,{audioBitsPerSecond:128000},name,offset);
        t.entry.key=key;this.recorders.push(t.entry);
      }
      t.name=name;t.entry.label=name;
      node.connect(t.dest);
      this.sources.set(key,{node,track});
    }
    this.mixGain.gain.value=1/Math.max(1,Math.sqrt(this.sources.size));
    // Videoelementer for tegning
    const visuals=this.getVisuals();const want=new Set();
    for(const v of visuals)if(v.stream){
      const id=v.kind+':'+v.key;want.add(id);
      let o=this.videos.get(id);
      if(!o||o.stream!==v.stream){
        if(o)o.el.srcObject=null;
        const el=document.createElement('video');el.muted=true;el.playsInline=true;el.autoplay=true;el.srcObject=v.stream;el.play().catch(()=>{});
        o={el,stream:v.stream};this.videos.set(id,o);
      }
    }
    for(const [id,o] of this.videos)if(!want.has(id)){o.el.srcObject=null;this.videos.delete(id);}
  }

  draw(){
    const g=this.g,W=this.W,H=this.H;
    g.fillStyle='#162e35';g.fillRect(0,0,W,H);
    let visuals=[];try{visuals=this.getVisuals();}catch(e){}
    const screens=visuals.filter(v=>v.kind==='screen'),people=visuals.filter(v=>v.kind!=='screen');
    const pad=12;
    if(screens.length){
      // Delt skjerm stort, deltakerne i en kolonne til høyre.
      const side=people.length?Math.round(W*0.22):0;
      this.tile(screens[0],pad,pad,W-side-pad*(side?3:2),H-pad*2,true);
      if(side){
        const n=Math.min(people.length,5),h=Math.min((H-pad*(n+1))/n,side*9/16+40);
        people.slice(0,5).forEach((p,i)=>this.tile(p,W-side-pad,pad+i*(h+pad),side,h,false));
      }
    }else if(people.length){
      const n=people.length,cols=Math.ceil(Math.sqrt(n)),rows=Math.ceil(n/cols);
      const tw=(W-pad*(cols+1))/cols,th=(H-pad*(rows+1))/rows;
      people.forEach((p,i)=>{
        const r=Math.floor(i/cols),inRow=r===rows-1?n-r*cols:cols;
        const x0=(W-(inRow*tw+(inRow-1)*pad))/2;
        this.tile(p,x0+(i%cols)*(tw+pad),pad+r*(th+pad),tw,th,false);
      });
    }
    // Liten tidskode/merke nederst til høyre
    g.font='600 16px system-ui,sans-serif';g.textAlign='right';g.textBaseline='bottom';
    g.fillStyle='rgba(255,255,255,.55)';g.fillText('møtes.no',W-pad-4,H-pad);
    g.textAlign='left';
  }

  tile(v,x,y,w,h,contain){
    const g=this.g;
    g.save();this.round(x,y,w,h,12);g.clip();
    g.fillStyle='#0f2328';g.fillRect(x,y,w,h);
    const o=v.stream&&this.videos.get(v.kind+':'+v.key);
    const el=o?.el;
    if(el&&el.readyState>=2&&el.videoWidth){
      const vw=el.videoWidth,vh=el.videoHeight;
      const s=contain?Math.min(w/vw,h/vh):Math.max(w/vw,h/vh);
      const dw=vw*s,dh=vh*s,dx=x+(w-dw)/2,dy=y+(h-dh)/2;
      if(v.mirror){g.translate(dx+dw,dy);g.scale(-1,1);g.drawImage(el,0,0,dw,dh);g.setTransform(1,0,0,1,0,0);}
      else g.drawImage(el,dx,dy,dw,dh);
    }else if(v.kind!=='screen'){
      // Ingen video: initialer i en sirkel
      const r=Math.min(w,h)*0.18;
      g.fillStyle='#e8f1ed';g.beginPath();g.arc(x+w/2,y+h/2-8,r,0,Math.PI*2);g.fill();
      g.fillStyle='#216052';g.font='700 '+Math.round(r*0.8)+'px system-ui,sans-serif';g.textAlign='center';g.textBaseline='middle';
      const p=(v.name||'?').trim().split(/\s+/);g.fillText(((p[0]||'?')[0]+(p.length>1?p[p.length-1][0]:'')).toUpperCase(),x+w/2,y+h/2-8);
      g.textAlign='left';
    }
    g.restore();
    if(v.speaking){g.save();g.strokeStyle='#46b39a';g.lineWidth=4;this.round(x+2,y+2,w-4,h-4,11);g.stroke();g.restore();}
    if(v.name){
      g.save();g.font='600 15px system-ui,sans-serif';g.textBaseline='middle';
      const label=v.kind==='screen'?v.name+' deler skjerm':v.name;
      const tw=Math.min(g.measureText(label).width+18,w-16);
      g.fillStyle='rgba(0,0,40,.72)';this.round(x+8,y+h-36,tw,28,7);g.fill();
      g.fillStyle='#fff';g.fillText(label,x+17,y+h-22,tw-18);g.restore();
    }
  }
  round(x,y,w,h,r){const g=this.g;g.beginPath();g.moveTo(x+r,y);g.arcTo(x+w,y,x+w,y+h,r);g.arcTo(x+w,y+h,x,y+h,r);g.arcTo(x,y+h,x,y,r);g.arcTo(x,y,x+w,y,r);g.closePath();}

  async stop(){
    if(this.stopped)return null;this.stopped=true;
    const duration=this.elapsed();
    clearInterval(this.syncTimer);
    try{this.worker?.postMessage(0);this.worker?.terminate();}catch(e){}
    if(this.workerUrl)URL.revokeObjectURL(this.workerUrl);
    this.draw();
    for(const r of this.recorders)try{if(r.rec.state!=='inactive'){r.rec.requestData();r.rec.stop();}}catch(e){}
    await Promise.all(this.recorders.map(r=>r.rec.state==='inactive'&&!r.chunks.length?null:Promise.race([r.done,new Promise(s=>setTimeout(s,4000))])));
    for(const s of this.sources.values())try{s.node.disconnect();}catch(e){}
    this.sources.clear();
    for(const o of this.videos.values())o.el.srcObject=null;
    this.videos.clear();
    try{await this.ctx.close();}catch(e){}
    const stamp=this.startDate.toISOString().slice(0,16).replace('T','_').replace(':','');
    const safe=s=>String(s||'deltaker').normalize('NFKD').replace(/[̀-ͯ]/g,'').replace(/æ/gi,'ae').replace(/ø/gi,'o').replace(/[^a-z0-9]+/gi,'-').replace(/^-|-$/g,'').slice(0,30)||'deltaker';
    const fmt=s=>{s=Math.max(0,Math.round(s));const h=Math.floor(s/3600),m=Math.floor(s%3600/60),x=s%60;return (h?h+'t':'')+String(m).padStart(2,'0')+'m'+String(x).padStart(2,'0')+'s';};
    let n=0;const files=[];
    for(const r of this.recorders){
      if(!r.chunks.length)continue;
      const blob=new Blob(r.chunks,{type:r.mime.split(';')[0]});
      const ext=MeetingRecorder.ext(r.mime);
      let name,label=r.label;
      if(r.kind==='video')name='motes-'+stamp+'-video.'+ext;
      else if(r.kind==='mix')name='motes-'+stamp+'-lyd-miks.'+ext;
      else{
        n++;
        name='motes-'+stamp+'-spor'+String(n).padStart(2,'0')+'-'+safe(r.label)+(r.offset>=1?'-starter-'+fmt(r.offset):'')+'.'+ext;
        label='Lydspor: '+r.label+(r.offset>=1?' (starter '+fmt(r.offset)+' ut i opptaket)':'');
      }
      files.push({kind:r.kind,name,label,blob,offset:r.offset});
    }
    this.recorders=[];this.tracks.clear();
    return {files,duration,startDate:this.startDate};
  }
};
