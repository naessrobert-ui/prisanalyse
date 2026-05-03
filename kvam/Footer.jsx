const Footer = () => (
  <footer className="kk-footer">
    <div className="container">
      <div>
        <div style={{display:'flex', alignItems:'center', gap:8, color:'#fff'}}>
          <Spruce size={26} style={{color:'#fff'}}/>
          <span style={{fontFamily:'var(--font-display)', fontSize:24, fontWeight:500, letterSpacing:'-0.01em'}}>Kvamskogen</span>
        </div>
        <p className="blurb">Eit hyttelandskap mellom Hardanger og Bergen — drevet på dugnad sidan 1971.</p>
        <a className="donate" href="https://www.kvamskogen-vel.no/" target="_blank" rel="noopener"><Icon name="heart" size={14}/> Bli medlem av Kvamskogen Vel</a>
      </div>
      <div>
        <h4>Området</h4>
        <ul>
          <li><a href="https://prisanalyse.no/kvamskogen/" target="_blank" rel="noopener">Vêr og føreforhold</a></li>
          <li><a href="/ver/skiloyper-kvamskogen" target="_blank" rel="noopener">Preparerte skiløyper</a></li>
          <li><a href="https://www.kvamskogen-vel.no/wp-content/uploads/2025/08/Lavlandsloypen-over-Kvamskogen-pr-juli-2025-4.pdf" target="_blank" rel="noopener">Lavlandsløypen (PDF)</a></li>
          <li><a>Webkamera</a></li>
          <li><a>Skisentre</a></li>
          <li><a>Parkering</a></li>
        </ul>
      </div>
      <div>
        <h4>For hyttefolk</h4>
        <ul>
          <li><a>Tilskot til løypeprep.</a></li>
          <li><a>Plansaker og høringar</a></li>
          <li><a>Vatn og avlaup</a></li>
          <li><a>Reguleringsplaner</a></li>
        </ul>
      </div>
      <div>
        <h4>Kontakt</h4>
        <ul>
          <li><a href="https://www.kvamskogen-vel.no/" target="_blank" rel="noopener">Kvamskogen Vel ↗</a></li>
          <li><a>Kvam herad</a></li>
          <li><a>Bergen og Omland Friluftsråd</a></li>
          <li><a>Furedalen Alpin</a></li>
        </ul>
      </div>
      <div className="legal">
        <span>© 2026 Kvamskogen Vel</span>
        <span>Org. 971 432 109</span>
        <span style={{marginLeft:'auto'}}>Driftet på dugnad. Foto: lokale fotografar.</span>
      </div>
    </div>
  </footer>
);
window.Footer = Footer;
