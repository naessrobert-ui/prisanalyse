const ActivityCard = ({ a }) => (
  <article className="activity-card">
    <div className="img">
      <div className={"photo " + a.photo}/>
      <span className="img-tag">{a.seasonLabel}</span>
    </div>
    <div className="body">
      <div className="eb">{a.kicker}</div>
      <div className="t">{a.title}</div>
      <div className="meta">
        <span>{a.distance}</span>
        <span className="sep">·</span>
        <span>{a.difficulty}</span>
        <span className="sep">·</span>
        <span>{a.duration}</span>
      </div>
    </div>
  </article>
);
window.ActivityCard = ActivityCard;
