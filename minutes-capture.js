// Standalone capture helper: one mixed audio stream, no video, no paid live AI.
window.MeetingMinutesCapture = class {
  constructor(onChunk,onFailure){this.onChunk=onChunk;this.onFailure=onFailure;this.sources=new Map();this.parts=[];this.samples=0;this.stopped=false;}
  async start(){
    this.context=new (window.AudioContext||window.webkitAudioContext)({sampleRate:16000});
    if(this.context.sampleRate!==16000||!this.context.audioWorklet)throw Error('Denne nettleseren støtter ikke lydformatet for referat. Prøv oppdatert Chrome.');
    const code=`class MinutesPCM extends AudioWorkletProcessor{constructor(){super();this.p=new Float32Array(4096);this.n=0;}process(inputs,outputs){const a=inputs[0]?.[0];if(a)for(let i=0;i<a.length;i++){this.p[this.n++]=a[i];if(this.n===4096){this.port.postMessage(this.p);this.p=new Float32Array(4096);this.n=0;}}return true;}}registerProcessor('minutes-pcm',MinutesPCM);`;
    const url=URL.createObjectURL(new Blob([code],{type:'text/javascript'}));
    try{await this.context.audioWorklet.addModule(url);}finally{URL.revokeObjectURL(url);}
    if(this.stopped){await this.context.close();return;}
    this.mix=this.context.createGain();this.node=new AudioWorkletNode(this.context,'minutes-pcm',{channelCount:1,channelCountMode:'explicit'});
    this.silent=this.context.createGain();this.silent.gain.value=0;this.mix.connect(this.node);this.node.connect(this.silent);this.silent.connect(this.context.destination);
    this.node.port.onmessage=e=>{if(this.stopped)return;this.parts.push(e.data);this.samples+=e.data.length;if(this.samples>=320000)this.flush(false);};
    this.context.onstatechange=()=>{if(!this.stopped&&this.context.state==='suspended')this.onFailure('Nettleseren pauset lyden. Referatet er stoppet; det kan være ufullstendig.');};
    await this.context.resume();
  }
  update(streams){
    if(!this.mix||this.stopped)return;
    const tracks=new Map();
    for(const [id,stream] of streams){const track=stream?.getAudioTracks()[0];if(track&&track.readyState==='live')tracks.set(id,{stream,track});}
    for(const [id,entry] of this.sources)if(tracks.get(id)?.track!==entry.track){entry.source.disconnect();this.sources.delete(id);}
    for(const [id,{stream,track}] of tracks)if(!this.sources.has(id)){const source=this.context.createMediaStreamSource(stream);source.connect(this.mix);this.sources.set(id,{source,track});}
    this.mix.gain.value=1/Math.max(1,Math.sqrt(this.sources.size));
  }
  flush(final){
    const count=final?this.samples:Math.min(this.samples,320000);if(!count)return;
    const all=new Float32Array(this.samples);let offset=0;for(const p of this.parts){all.set(p,offset);offset+=p.length;}
    // Keep a one-second overlap so words spanning a boundary can be recovered.
    const remain=final?new Float32Array(0):all.slice(Math.max(0,count-16000));this.parts=remain.length?[remain]:[];this.samples=remain.length;
    if(final&&count<4000)return;
    const buffer=new ArrayBuffer(44+count*2),v=new DataView(buffer);
    const str=(at,text)=>{for(let i=0;i<text.length;i++)v.setUint8(at+i,text.charCodeAt(i));};
    str(0,'RIFF');v.setUint32(4,36+count*2,true);str(8,'WAVE');str(12,'fmt ');v.setUint32(16,16,true);v.setUint16(20,1,true);v.setUint16(22,1,true);v.setUint32(24,16000,true);v.setUint32(28,32000,true);v.setUint16(32,2,true);v.setUint16(34,16,true);str(36,'data');v.setUint32(40,count*2,true);
    for(let i=0;i<count;i++)v.setInt16(44+i*2,Math.round(Math.max(-1,Math.min(1,all[i]))*32767),true);
    this.onChunk(new Blob([buffer],{type:'audio/wav'}));
  }
  async stop(flush=false){
    if(this.stopped)return;this.stopped=true;if(flush)this.flush(true);
    this.parts=[];this.samples=0;for(const e of this.sources.values())try{e.source.disconnect();}catch(e){}this.sources.clear();
    if(this.node){this.node.port.onmessage=null;this.node.disconnect();}this.mix?.disconnect();this.silent?.disconnect();
    if(this.context&&this.context.state!=='closed')await this.context.close();
  }
};
