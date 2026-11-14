'use strict';
self.addEventListener('push',event=>{
 let data={};try{data=event.data.json();}catch(e){}
 event.waitUntil(self.registration.showNotification('Nytt i familierommet',{
  body:'Familien har delt noe nytt. Åpne familierommet for å se det.',
  tag:'family-first-new',renotify:false,icon:'/familie/icon.png',badge:'/familie/icon.png',data:{url:'/familie/'}
 }));
});
self.addEventListener('notificationclick',event=>{
 event.notification.close();
 event.waitUntil((async()=>{const windows=await clients.matchAll({type:'window',includeUncontrolled:true});for(const client of windows){if(new URL(client.url).pathname.startsWith('/familie')){await client.focus();await client.navigate('/familie/');return;}}await clients.openWindow('/familie/');})());
});
