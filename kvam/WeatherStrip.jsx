const WeatherStrip = ({ data }) => (
  <div className="weather-strip">
    <div className="weather-cell">
      <div className="eb">{data.station}</div>
      <div className="val">
        <span className="num cold">{data.temp}</span>
        <span className="cap">{data.cond}</span>
      </div>
    </div>
    <div className="weather-div"/>
    <div className="weather-cell">
      <div className="eb">Snødybde</div>
      <div className="val"><span className="num">{data.snow}</span><span className="unit">cm</span></div>
    </div>
    <div className="weather-div"/>
    <div className="weather-cell">
      <div className="eb">Vind</div>
      <div className="val"><span className="num">{data.wind}</span><span className="unit">m/s {data.windDir}</span></div>
    </div>
    <div className="weather-foot">
      <Icon name="clock" size={14}/>
      <span>Oppdatert kl. {data.updated}</span>
      <span style={{color:'var(--color-border-strong)'}}>·</span>
      <a href="https://prisanalyse.no/kvamskogen/" target="_blank" rel="noopener">se mer →</a>
    </div>
  </div>
);
window.WeatherStrip = WeatherStrip;
