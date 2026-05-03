const TRAILS = [
  { n:1, name:'Furedalen → Mødal',          sub:'Maskinpreparert til foten av Såta', km:'8,4 km', status:'ok',   tag:'Preparert i dag' },
  { n:2, name:'Løkjentunet → Mødal',        sub:'Familievennleg, jamn stigning',       km:'6,1 km', status:'ok',   tag:'Preparert' },
  { n:3, name:'Aktiven → Steinskvanndalen', sub:'Lite snø under 500 moh.',             km:'4,7 km', status:'warn', tag:'Delvis' },
  { n:4, name:'Lavlandsløypen rundt',       sub:'Open hele året — våt etter regn',     km:'4,2 km', status:'ok',   tag:'Open' },
  { n:5, name:'Aktiven → Tveitakvitingen',  sub:'Topptur. Vurder vêret før avgang.',   km:'5,8 km', status:'bad',  tag:'Stengt' },
];

const TrailList = ({ onSelect }) => (
  <section className="section tight">
    <div className="container">
      <div className="section-head">
        <div className="titles">
          <div className="eyebrow winter"><span className="dot"/>Løyper og stier · oppdatert i går kl. 22.30</div>
          <h2>Hva er preparert akkurat nå?</h2>
          <p className="lede">Status kjem direkte frå løypemaskinene og BOF. Trykk på ei løype for kart og høgdeprofil.</p>
        </div>
        <button className="btn btn-secondary">Last ned løypekart (PDF)</button>
      </div>
      <div className="trail-banner">
        <div className="quote">
          <small>Frå dugnaden</small>
          <p>"Det som gjer at maskinene kjem ut laurdagskvelden, er at hyttefolket bidreg med tilskot."</p>
        </div>
      </div>
      <div className="trail-card">
        {TRAILS.map(t => (
          <div className="trail-row" key={t.n} onClick={() => onSelect && onSelect(t)}>
            <div className="pin">{t.n}</div>
            <div>
              <div className="name">{t.name}</div>
              <div className="sub">{t.sub}</div>
            </div>
            <div className="km">{t.km}</div>
            <span className={"tag tag-" + t.status}><span className="dot"/>{t.tag}</span>
            <div className="arrow">→</div>
          </div>
        ))}
      </div>
    </div>
  </section>
);
window.TrailList = TrailList;
