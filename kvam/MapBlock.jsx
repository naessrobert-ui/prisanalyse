const MapBlock = () => {
  const pins = [
    { x: 22, y: 35, color: '#B23A2C' }, // accent — recommended
    { x: 48, y: 25, color: '#1F5C7C' },
    { x: 62, y: 48, color: '#1F5C7C' },
    { x: 38, y: 58, color: '#6F8A3C' },
    { x: 75, y: 30, color: '#6F8A3C' },
  ];
  return (
    <section className="section">
      <div className="container map-block">
        <div className="map-frame">
          <div className="map-pins">
            {pins.map((p,i) => (
              <span key={i} className="map-pin" style={{ left: p.x + '%', top: p.y + '%' }}>
                <svg viewBox="0 0 24 32"><path d="M12 1c-5 0-9 3.8-9 8.6 0 6.6 9 21 9 21s9-14.4 9-21C21 4.8 17 1 12 1z" fill={p.color}/><circle cx="12" cy="9.4" r="3.6" fill="#FBFAF6"/></svg>
              </span>
            ))}
          </div>
          <div className="map-attrib">© BOF · Kvam herad</div>
        </div>
        <div className="map-text">
          <div className="eyebrow summer"><span className="dot"/>Kart og GPS</div>
          <h3>Eitt felles løypekart for heile Kvamskogen.</h3>
          <p>Kartet viser dei preparerte vinterløypene, sommarstiar og parkering. Det er òg tilgjengeleg i postkassane på dei store infotavlene på parkeringsplassane.</p>
          <ul>
            <li>Sporloggar (GPX) frå Bergen og Omland Friluftsråd</li>
            <li>Forslag til rundturar med løypebeskrivelsar</li>
            <li>Kva løype som blir preparert kva dag</li>
            <li>Avenza-kart for offline bruk på fjellet</li>
          </ul>
          <div style={{display:'flex', gap:10, flexWrap:'wrap'}}>
            <a className="btn btn-primary" href="/ver/skiloyper-kvamskogen" target="_blank" rel="noopener">Preparerte skiløyper</a>
            <a className="btn btn-secondary" href="https://www.kvamskogen-vel.no/wp-content/uploads/2025/08/Lavlandsloypen-over-Kvamskogen-pr-juli-2025-4.pdf" target="_blank" rel="noopener">Lavlandsløypen (PDF)</a>
          </div>
        </div>
      </div>
    </section>
  );
};
window.MapBlock = MapBlock;
