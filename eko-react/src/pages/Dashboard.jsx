import { useEffect, useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { getPlayerAuth } from './Login';
import { getMemberDues, submitPaymentEvidence, applyWaiver, listMemberMatchdays, getMemberMatchday, voteMatchday, getMemberStats, getMemberTopThreeBallers, invalidateStatsCache } from '../api';
import JerseyAvatar from '../components/JerseyAvatar';
import { useToast } from '../components/Toast';
import { TopFiveSkeleton } from '../components/Skeleton';
import { getSavedJersey, JerseySVG, fetchKitImage, fetchTeamFanart, FULL_TEAM_NAMES } from './JerseyDesigner';
import './Dashboard.css';

function useCountdown(sundayDateStr) {
  const [left, setLeft] = useState({ days: 0, hours: 0, mins: 0 });
  useEffect(() => {
    if (!sundayDateStr) return;
    const kickoff = new Date(sundayDateStr + 'T18:00:00');
    const tick = () => {
      const now = new Date();
      let ms = kickoff - now;
      if (ms <= 0) { setLeft({ days: 0, hours: 0, mins: 0 }); return; }
      const days = Math.floor(ms / (24 * 60 * 60 * 1000));
      ms -= days * 24 * 60 * 60 * 1000;
      const hours = Math.floor(ms / (60 * 60 * 1000));
      ms -= hours * 60 * 60 * 1000;
      const mins = Math.floor(ms / (60 * 1000));
      setLeft({ days, hours, mins });
    };
    tick();
    const id = setInterval(tick, 60 * 1000);
    return () => clearInterval(id);
  }, [sundayDateStr]);
  return left;
}

export default function Dashboard() {
  const navigate = useNavigate();
  const { token, player } = getPlayerAuth();
  const toast = useToast();
  const [dues, setDues] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [waiverDue, setWaiverDue] = useState('');
  const [matchdays, setMatchdays] = useState([]);
  const [featuredMatchday, setFeaturedMatchday] = useState(null);
  const [voting, setVoting] = useState(false);
  const [memberStats, setMemberStats] = useState(null);
  const [statsLoading, setStatsLoading] = useState(true);
  const [topThreeBallers, setTopThreeBallers] = useState([]);
  const [topThreeLoading, setTopThreeLoading] = useState(true);
  const [heroKitImageUrl, setHeroKitImageUrl] = useState(null);
  const [heroFanartUrl, setHeroFanartUrl] = useState(null);
  const fileInputRef = useRef(null);

  // Fetch real kit photo + team fanart for hero
  useEffect(() => {
    const saved = getSavedJersey();
    if (!saved?.sportsDbId) return;
    const kitLabel = { home: 'Home', away: 'Away', third: 'Third' }[saved.kitType] || 'Home';
    fetchKitImage(saved.sportsDbId, kitLabel).then(url => setHeroKitImageUrl(url || null));
    fetchTeamFanart(saved.sportsDbId).then(url => setHeroFanartUrl(url || null));
  }, []);

  useEffect(() => {
    if (!token) return;
    setStatsLoading(true);
    setTopThreeLoading(true);
    Promise.allSettled([getMemberStats(token), getMemberTopThreeBallers(token)]).then(([statsRes, topRes]) => {
      setMemberStats(statsRes.status === 'fulfilled' ? statsRes.value : null);
      setStatsLoading(false);
      setTopThreeBallers(topRes.status === 'fulfilled' ? topRes.value?.top_three || [] : []);
      setTopThreeLoading(false);
    });
  }, [token]);

  useEffect(() => {
    if (!token) return;
    const refetch = () => {
      if (document.visibilityState === 'visible') {
        invalidateStatsCache();
        Promise.allSettled([getMemberStats(token), getMemberTopThreeBallers(token)]).then(([statsRes, topRes]) => {
          if (statsRes.status === 'fulfilled') setMemberStats(statsRes.value);
          if (topRes.status === 'fulfilled') setTopThreeBallers(topRes.value?.top_three || []);
        });
      }
    };
    document.addEventListener('visibilitychange', refetch);
    window.addEventListener('focus', refetch);
    return () => {
      document.removeEventListener('visibilitychange', refetch);
      window.removeEventListener('focus', refetch);
    };
  }, [token]);

  useEffect(() => {
    if (!token) return;
    getMemberDues(token)
      .then(setDues)
      .catch(() => setDues({ status: 'owing', pending_evidence: false, year: new Date().getFullYear(), quarter: Math.ceil((new Date().getMonth() + 1) / 3) }));
  }, [token]);

  useEffect(() => {
    if (!token) return;
    listMemberMatchdays(token)
      .then((d) => setMatchdays(d.matchdays || []))
      .catch(() => setMatchdays([]));
  }, [token]);

  useEffect(() => {
    if (!token || matchdays.length === 0) { setFeaturedMatchday(null); return; }
    const next = matchdays.find((md) => !md.matchday_ended && md.status !== 'rejected' && (md.status === 'voting_open' || md.status === 'approved'));
    if (!next) { setFeaturedMatchday(null); return; }
    getMemberMatchday(next.id, token).then(setFeaturedMatchday).catch(() => setFeaturedMatchday(null));
  }, [token, matchdays]);

  const handleSendEvidence = () => fileInputRef.current?.click();

  const handleFileChange = async (e) => {
    const file = e.target.files?.[0];
    if (!file || !token) return;
    e.target.value = '';
    setUploading(true);
    try {
      const data = await submitPaymentEvidence(file, token);
      toast(data.message || 'Evidence submitted. Admin will review.', 'success');
      getMemberDues(token).then(setDues);
    } catch (err) {
      toast(err.response?.data?.detail || 'Upload failed.', 'error');
    } finally {
      setUploading(false);
    }
  };

  const handleApplyWaiver = async (e) => {
    e.preventDefault();
    if (!waiverDue || !token) return;
    try {
      await applyWaiver(waiverDue, token);
      toast('Waiver applied. Pay by ' + waiverDue, 'success');
      getMemberDues(token).then(setDues);
    } catch (err) {
      toast(err.response?.data?.detail || 'Failed.', 'error');
    }
  };

  const handleVote = async () => {
    if (!featuredMatchday?.matchday?.id || !token) return;
    setVoting(true);
    try {
      await voteMatchday(featuredMatchday.matchday.id, token);
      toast('Vote recorded!', 'success');
      getMemberMatchday(featuredMatchday.matchday.id, token).then(setFeaturedMatchday);
    } catch (err) {
      toast(err.response?.data?.detail || 'Vote failed.', 'error');
    } finally {
      setVoting(false);
    }
  };

  if (!player) return null;

  const quarterLabels = { 1: 'Jan–Mar', 2: 'Apr–Jun', 3: 'Jul–Sep', 4: 'Oct–Dec' };
  const period = dues ? `${dues.year} Q${dues.quarter} (${quarterLabels[dues.quarter] || ''})` : '—';
  const md = featuredMatchday?.matchday;
  const hasVoting = md?.status === 'voting_open';
  const countdown = useCountdown(md?.sunday_date);
  const voteCount = featuredMatchday?.vote_count ?? 0;
  const stats = memberStats?.stats;
  const playerName = stats?.baller_name || player?.baller_name || 'Baller';
  const level = stats?.matchdays_present ?? 0;
  const recentMatchdays = matchdays.filter(m => m.matchday_ended).slice(0, 3);
  const savedJersey = getSavedJersey();

  return (
    <div className="bg-background-light dark:bg-background-dark text-slate-900 dark:text-slate-100 min-h-screen flex flex-col font-display">
      <main className="flex-1 pb-6">
        {/* ── Page heading (desktop only — mobile uses MemberLayout header) ── */}
        <div className="hidden md:flex items-center justify-between px-4 pt-6 pb-2">
          <div>
            <h2 className="text-xl font-black text-slate-900 dark:text-white tracking-tight">Player Hub</h2>
            <p className="text-xs text-slate-500">Welcome back, {playerName}</p>
          </div>
          {dues?.status === 'owing' && (
            <span className="text-[10px] font-bold text-red-400 bg-red-500/10 border border-red-500/20 rounded-lg px-2.5 py-1 uppercase tracking-widest">Dues owing</span>
          )}
        </div>

        {/* ── Hero + Top 3: shared row ── */}
        <div className="px-4 py-3 md:py-4 flex flex-col md:flex-row items-stretch gap-4">

        {/* ── Hero: Player Card ── */}
        <div className="md:flex-[3] min-w-0">
          <div
            className="relative w-full h-full rounded-xl overflow-hidden border border-slate-200 dark:border-primary/10"
            style={{ minHeight: '380px' }}
          >
            {/* Layer 1: base dark background */}
            <div className="absolute inset-0" style={{ background: '#080d09' }} />

            {/* Layer 2: fanart photo (when available) */}
            {heroFanartUrl && (
              <>
                <div className="absolute inset-0" style={{ backgroundImage: `url(${heroFanartUrl})`, backgroundSize: 'cover', backgroundPosition: 'center top' }} />
                <div className="absolute inset-0 bg-gradient-to-r from-black/88 via-black/55 to-black/25" />
              </>
            )}

            {/* Layer 3: stadium spotlight + team color glows */}
            <div className="absolute inset-0 overflow-hidden pointer-events-none">
              {/* Stadium top spotlight */}
              <div className="absolute inset-x-0 -top-10 h-64" style={{ background: 'radial-gradient(ellipse 75% 55% at 50% 0%, rgba(255,255,255,0.06) 0%, transparent 70%)' }} />
              {savedJersey && (
                <>
                  <div className="absolute -right-10 -top-10 w-80 h-80 rounded-full"
                       style={{ background: `radial-gradient(circle, ${savedJersey.teamColor || '#0ac247'}50 0%, transparent 65%)`, filter: 'blur(4px)' }} />
                  <div className="absolute -left-10 bottom-0 w-56 h-56 rounded-full"
                       style={{ background: `radial-gradient(circle, ${savedJersey.teamColor || '#0ac247'}20 0%, transparent 70%)` }} />
                </>
              )}
            </div>

            {/* Content: left jersey + right stats */}
            <div className="relative z-10 flex" style={{ minHeight: '380px' }}>

              {/* Left: club name + jersey (centered) */}
              <div className="flex-1 flex flex-col items-center justify-center text-center px-4 pt-10 pb-14">
                {savedJersey?.teamId && (
                  <div className="mb-2">
                    <p className="text-[10px] font-black uppercase tracking-[0.25em] text-primary mb-0.5">Club</p>
                    <h2
                      className="font-black text-white uppercase leading-tight"
                      style={{ fontSize: 'clamp(13px, 3.8vw, 20px)', textShadow: '0 2px 16px rgba(0,0,0,0.9)' }}
                    >
                      {FULL_TEAM_NAMES[savedJersey.teamId] || savedJersey.teamName || ''}
                    </h2>
                    <span className="mt-1.5 inline-block text-[9px] font-bold text-primary uppercase tracking-widest border border-primary/40 rounded px-2 py-0.5 bg-primary/10">
                      {savedJersey.kitType} Kit
                    </span>
                  </div>
                )}
                {savedJersey?.kit ? (
                  heroKitImageUrl ? (
                    <>
                      {(savedJersey.playerName || savedJersey.playerNumber) && (
                        <div className="flex flex-col items-center leading-none mb-1">
                          {savedJersey.playerName && (
                            <span className="font-black tracking-[0.22em] text-white uppercase" style={{ fontSize: 13, textShadow: '0 2px 8px rgba(0,0,0,0.9)' }}>
                              {savedJersey.playerName.toUpperCase().slice(0, 11)}
                            </span>
                          )}
                          {savedJersey.playerNumber && (
                            <span className="font-black text-white" style={{ fontSize: 40, lineHeight: 1, textShadow: '0 2px 14px rgba(0,0,0,0.9)' }}>
                              {savedJersey.playerNumber}
                            </span>
                          )}
                        </div>
                      )}
                      <img src={heroKitImageUrl} alt="kit" className="object-contain drop-shadow-2xl" style={{ width: 185, height: 185 }} onError={() => setHeroKitImageUrl(null)} />
                    </>
                  ) : (
                    <JerseySVG kit={savedJersey.kit} playerName={savedJersey.playerName || playerName} playerNumber={savedJersey.playerNumber || ''} size={185} />
                  )
                ) : (
                  <span className="material-symbols-outlined text-primary opacity-20" style={{ fontSize: '80px' }}>apparel</span>
                )}
              </div>

              {/* Right: Stats panel — bottom aligned */}
              <div className="w-[148px] shrink-0 flex flex-col justify-end pb-14 pr-3 pt-8 pl-1">
                {statsLoading ? (
                  <div className="grid grid-cols-2 gap-1.5">
                    {[1,2,3,4,5,6].map(i => <div key={i} className="h-10 rounded-lg bg-white/5 animate-pulse" />)}
                  </div>
                ) : stats ? (
                  <>
                    <div className="grid grid-cols-2 gap-1.5 mb-1.5">
                      {[
                        { label: 'Goals',   value: stats.goals ?? 0 },
                        { label: 'Assists', value: stats.assists ?? 0 },
                        { label: 'Present', value: stats.matchdays_present ?? 0 },
                        { label: 'Avg Rtg', value: stats.average_rating ?? '—' },
                        { label: 'C/Sheet', value: stats.clean_sheets ?? 0 },
                        { label: 'Yellows', value: stats.yellow_cards ?? 0 },
                      ].map(({ label, value }) => (
                        <div key={label} className="flex flex-col items-center bg-white/5 border border-white/8 rounded-lg py-1.5 px-1">
                          <span className="text-[8px] text-slate-400 uppercase font-bold tracking-wide leading-tight mb-0.5">{label}</span>
                          <span className="text-sm font-black text-white leading-tight">{value}</span>
                        </div>
                      ))}
                    </div>
                    <div className="flex items-center justify-between bg-amber-500/10 border border-amber-500/25 rounded-lg px-2.5 py-1.5">
                      <span className="text-[9px] text-amber-400 uppercase font-black tracking-wide">MOTM</span>
                      <span className="text-base font-black text-amber-400 leading-none">{stats.motm_count ?? 0}</span>
                    </div>
                    {memberStats?.global_rank && (
                      <button onClick={() => navigate('/leaderboard')} className="mt-1.5 text-[9px] text-primary font-bold uppercase tracking-widest text-right w-full">
                        #{memberStats.global_rank} Global →
                      </button>
                    )}
                  </>
                ) : null}
              </div>
            </div>

            {/* Rating badge — top left */}
            {stats?.average_rating && (
              <div className="absolute top-3 left-3 z-20 bg-primary/90 text-white rounded-lg px-3 py-1 text-xs font-black uppercase tracking-widest">
                ★ {stats.average_rating}
              </div>
            )}

            {/* Pick kit CTA (no jersey) */}
            {!savedJersey && (
              <button
                onClick={() => navigate('/jersey')}
                className="absolute top-3 right-3 z-20 bg-primary/80 text-white rounded-lg px-3 py-1.5 text-xs font-bold flex items-center gap-1 backdrop-blur-sm"
              >
                <span className="material-symbols-outlined text-sm">checkroom</span>
                Pick your kit
              </button>
            )}

            {/* Change kit — small round icon, top right, won't overlap jersey */}
            {savedJersey && (
              <button
                onClick={() => navigate('/jersey')}
                className="absolute top-3 right-3 z-20 w-9 h-9 rounded-full bg-black/50 border border-white/15 flex items-center justify-center text-white/70 hover:text-white hover:border-white/30 transition-colors"
                aria-label="Change kit"
              >
                <span className="material-symbols-outlined" style={{ fontSize: 18 }}>edit</span>
              </button>
            )}

            {/* XP bar — pinned to bottom, always visible */}
            <div className="absolute bottom-0 left-0 right-0 z-20 px-5 pb-3 pt-8 bg-gradient-to-t from-black/85 to-transparent pointer-events-none">
              <div className="flex items-center gap-2 w-full max-w-xs">
                <div className="flex-1 h-1.5 bg-slate-700 rounded-full overflow-hidden">
                  <div className="h-full bg-primary rounded-full transition-all" style={{ width: `${Math.min(100, (level / 30) * 100)}%` }} />
                </div>
                <span className="text-xs text-primary font-bold">{level} games</span>
              </div>
            </div>
          </div>
        </div>

        {/* ── EKO TOP 3 BALLERS — Podium (second column) ── */}
        <div className="md:flex-[2] min-w-0">
          <div className="h-full bg-[#0d1117] border border-[#1e2433] rounded-xl p-4 flex flex-col" style={{ boxShadow: '0 0 40px rgba(251,191,36,0.06)' }}>
            <div className="flex justify-between items-start mb-1">
              <div>
                <h2 className="text-sm font-black uppercase tracking-wider text-amber-400">Top 3 Ballers</h2>
                <p className="text-[10px] text-slate-500">Season performance leaders</p>
              </div>
              <button onClick={() => navigate('/leaderboard')} className="text-primary text-[10px] font-bold uppercase hover:underline">Full Leaderboard</button>
            </div>
            {topThreeLoading && <TopFiveSkeleton />}
            {!topThreeLoading && topThreeBallers.length > 0 ? (
              <div className="flex items-end justify-center gap-2 flex-1 pt-4 pb-2">
                {/* #2 Silver */}
                {topThreeBallers[1] && (
                  <div className="flex flex-col items-center flex-1">
                    <div className="relative mb-2">
                      <JerseyAvatar shortName={topThreeBallers[1].baller_name} number={topThreeBallers[1].jersey_number} size="md" className="mx-auto" />
                      <span className="absolute -top-1 -right-1 w-5 h-5 bg-slate-400 rounded-full text-[9px] font-black text-black flex items-center justify-center">2</span>
                    </div>
                    <div className="w-full rounded-t-xl border-x border-t border-slate-500/30 flex flex-col items-center justify-end pb-2 pt-3 px-1" style={{ height: 96, background: 'linear-gradient(to top, rgba(148,163,184,0.15), rgba(148,163,184,0.05))' }}>
                      <p className="text-xs font-bold truncate w-full text-center text-slate-200">{topThreeBallers[1].baller_name}</p>
                      <p className="text-primary font-black text-sm">{topThreeBallers[1].average_rating}</p>
                      <p className="text-[9px] text-slate-500">G{topThreeBallers[1].goals} A{topThreeBallers[1].assists}</p>
                    </div>
                  </div>
                )}
                {/* #1 Gold — tallest */}
                {topThreeBallers[0] && (
                  <div className="flex flex-col items-center flex-1">
                    <div className="text-amber-400 text-lg text-center mb-0.5">👑</div>
                    <div className="relative mb-2">
                      <JerseyAvatar shortName={topThreeBallers[0].baller_name} number={topThreeBallers[0].jersey_number} size="lg" className="mx-auto ring-2 ring-amber-500/40 rounded-lg" />
                      <span className="absolute -top-1 -right-1 w-6 h-6 bg-amber-500 rounded-full text-[10px] font-black text-black flex items-center justify-center shadow-lg shadow-amber-500/30">1</span>
                    </div>
                    <div className="w-full rounded-t-xl border-x border-t border-amber-500/30 flex flex-col items-center justify-end pb-2 pt-3 px-1" style={{ height: 132, background: 'linear-gradient(to top, rgba(245,158,11,0.2), rgba(245,158,11,0.05))' }}>
                      <p className="text-sm font-bold truncate w-full text-center text-white">{topThreeBallers[0].baller_name}</p>
                      <p className="text-[9px] text-amber-400 uppercase font-black tracking-wider">Gold Rank</p>
                      <p className="text-primary font-black text-base">{topThreeBallers[0].average_rating}</p>
                      <p className="text-[9px] text-slate-400">G{topThreeBallers[0].goals} A{topThreeBallers[0].assists}</p>
                    </div>
                  </div>
                )}
                {/* #3 Bronze */}
                {topThreeBallers[2] && (
                  <div className="flex flex-col items-center flex-1">
                    <div className="relative mb-2">
                      <JerseyAvatar shortName={topThreeBallers[2].baller_name} number={topThreeBallers[2].jersey_number} size="md" className="mx-auto" />
                      <span className="absolute -top-1 -right-1 w-5 h-5 bg-amber-700 rounded-full text-[9px] font-black text-white flex items-center justify-center">3</span>
                    </div>
                    <div className="w-full rounded-t-xl border-x border-t border-amber-700/30 flex flex-col items-center justify-end pb-2 pt-3 px-1" style={{ height: 72, background: 'linear-gradient(to top, rgba(180,83,9,0.15), rgba(180,83,9,0.05))' }}>
                      <p className="text-xs font-bold truncate w-full text-center text-slate-200">{topThreeBallers[2].baller_name}</p>
                      <p className="text-primary font-black text-sm">{topThreeBallers[2].average_rating}</p>
                    </div>
                  </div>
                )}
              </div>
            ) : !topThreeLoading ? (
              <p className="text-slate-500 text-center py-8 text-sm flex-1 flex items-center justify-center">Play a matchday to see rankings.</p>
            ) : null}
          </div>
        </div>

        </div>{/* end hero+top3 row */}

        {/* ── Next Match + Voting (merged FIFA card) ── */}
        {md && (
          <div className="px-4 mb-6">
            <div className="relative rounded-xl overflow-hidden border border-[#1e2433] bg-[#0d1117]">
              {/* Background photo */}
              <div className="absolute inset-0" style={{ backgroundImage: "url('https://images.unsplash.com/photo-1574629810360-7efbbe195018?w=800')", backgroundSize: 'cover', backgroundPosition: 'center', opacity: 0.15 }} />
              <div className="absolute inset-0 bg-gradient-to-r from-[#0d1117] via-[#0d1117]/80 to-transparent" />
              {/* Glow */}
              <div className="absolute top-0 right-0 w-64 h-64 rounded-full pointer-events-none" style={{ background: 'radial-gradient(circle, rgba(10,194,71,0.08) 0%, transparent 70%)' }} />

              <div className="relative z-10 p-5 md:p-6 flex flex-col md:flex-row gap-6">
                {/* Left: match info + countdown */}
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-3">
                    <span className="px-2.5 py-1 bg-primary/20 border border-primary/30 text-primary text-[10px] font-black uppercase rounded-lg tracking-widest">Next Match</span>
                    <span className="text-slate-500 text-xs">Sunday {md.sunday_date} · 18:00</span>
                  </div>
                  <h3 className="text-xl font-black text-white mb-4">Eko Football Matchday</h3>
                  <div className="flex items-center gap-6">
                    {[
                      { val: countdown.days, lbl: 'Days' },
                      { val: countdown.hours, lbl: 'Hrs' },
                      { val: countdown.mins, lbl: 'Min' },
                    ].map(({ val, lbl }, idx) => (
                      <div key={lbl} className="flex items-center gap-6">
                        {idx > 0 && <span className="text-primary/30 text-xl font-black -ml-3">:</span>}
                        <div className="flex flex-col items-center">
                          <span className="text-3xl font-black text-white tabular-nums">{String(val).padStart(2, '0')}</span>
                          <span className="text-[9px] uppercase font-bold text-slate-500 tracking-widest">{lbl}</span>
                        </div>
                      </div>
                    ))}
                    <button type="button" onClick={() => navigate('/matchday')} className="ml-auto py-2 px-4 bg-primary text-black text-xs font-black rounded-xl flex items-center gap-1 shadow-lg shadow-primary/20">
                      <span className="material-symbols-outlined text-base">arrow_forward</span>
                      Details
                    </button>
                  </div>
                </div>

                {/* Right: attendance voting */}
                {hasVoting && (
                  <div className="md:w-64 shrink-0 border-t md:border-t-0 md:border-l border-[#1e2433] md:pl-6 pt-4 md:pt-0">
                    <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-3">Are you attending?</p>
                    <div className="flex gap-2 mb-4">
                      <button
                        type="button"
                        onClick={handleVote}
                        disabled={featuredMatchday?.voted || !featuredMatchday?.can_vote || voting}
                        className={`flex-1 py-2.5 text-xs font-black uppercase rounded-xl transition-all shadow-lg ${featuredMatchday?.voted ? 'bg-primary text-black shadow-primary/20' : 'bg-primary/90 text-black hover:bg-primary shadow-primary/10'}`}
                      >
                        {featuredMatchday?.voted ? '✓ In' : voting ? '...' : "I'm In"}
                      </button>
                      <button type="button" onClick={() => navigate('/matchday')} className="flex-1 py-2.5 text-xs font-black uppercase rounded-xl bg-white/5 border border-[#1e2433] text-slate-300 hover:bg-white/10 transition-all">
                        Maybe
                      </button>
                      <button type="button" onClick={() => navigate('/matchday')} className="flex-1 py-2.5 text-xs font-black uppercase rounded-xl bg-white/5 border border-[#1e2433] text-slate-400 hover:border-red-500/40 hover:text-red-400 transition-all">
                        Out
                      </button>
                    </div>
                    <div className="space-y-1.5">
                      <div className="flex justify-between text-[10px] font-bold">
                        <span className="text-slate-500 uppercase tracking-wider">Confirmed Squad</span>
                        <span className="text-primary">{voteCount} / 18</span>
                      </div>
                      <div className="h-2 bg-white/5 rounded-full overflow-hidden">
                        <div className="h-full bg-primary rounded-full transition-all shadow-sm shadow-primary/40" style={{ width: `${Math.min(100, (voteCount / 18) * 100)}%` }} />
                      </div>
                    </div>
                    {!featuredMatchday?.can_vote && !featuredMatchday?.voted && (
                      <p className="text-slate-500 text-[10px] mt-2">Only paid or waiver members can vote.</p>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* ── Recent Matchdays ── */}
        {recentMatchdays.length > 0 && (
          <div className="px-4 mb-6">
            <h3 className="text-lg font-bold mb-4">Recent Matchdays</h3>
            <div className="space-y-3">
              {recentMatchdays.map((m) => (
                <div
                  key={m.id}
                  onClick={() => navigate('/matchday')}
                  className="flex items-center justify-between p-4 bg-white dark:bg-slate-800/40 border border-slate-200 dark:border-slate-700/50 rounded-xl cursor-pointer hover:border-primary/40 transition-colors"
                >
                  <div className="flex items-center gap-3">
                    <div className="size-10 rounded-full bg-primary/10 flex items-center justify-center">
                      <span className="material-symbols-outlined text-primary text-lg">sports_soccer</span>
                    </div>
                    <div>
                      <p className="text-sm font-bold">Matchday {m.sunday_date}</p>
                      <p className="text-[10px] text-slate-500 uppercase font-bold">Ended</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-[10px] text-slate-500 dark:text-slate-400 uppercase font-bold bg-slate-100 dark:bg-slate-700/50 px-2 py-1 rounded">Finished</span>
                    <span className="material-symbols-outlined text-slate-400 text-base">chevron_right</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ── Dues ── */}
        <div className="px-4 mb-6">
          <div className="bg-white dark:bg-slate-800/40 border border-slate-200 dark:border-primary/20 rounded-xl p-5">
            <div className="flex items-center gap-2 mb-3">
              <span className="material-symbols-outlined text-primary">payments</span>
              <h3 className="font-bold">Quarterly Dues</h3>
              <span className="ml-auto text-xs text-slate-500">{period}</span>
            </div>
            <div className={`inline-block px-3 py-1.5 rounded-lg text-sm font-semibold mb-3 ${dues?.status === 'paid' ? 'bg-primary/20 text-primary' : dues?.status === 'waiver' ? 'bg-amber-500/20 text-amber-400' : 'bg-red-500/20 text-red-400'}`}>
              {dues?.status === 'paid' ? '✓ Paid' : dues?.status === 'waiver' ? 'Waiver granted' : 'Payment owing'}
            </div>
            {dues?.status === 'waiver' && dues?.waiver_due_by && (
              <p className="text-slate-500 text-xs mb-3">Pay by {dues.waiver_due_by}</p>
            )}
            {dues?.status !== 'paid' && !dues?.pending_evidence && (
              <>
                {dues?.status === 'owing' && (
                  <form onSubmit={handleApplyWaiver} className="flex flex-wrap items-end gap-3 mb-3">
                    <label className="block w-full text-xs text-slate-500">Apply for waiver (commit to pay by date)</label>
                    <input type="date" value={waiverDue} onChange={(e) => setWaiverDue(e.target.value)} required className="rounded-lg bg-slate-100 dark:bg-slate-700 border border-slate-300 dark:border-slate-600 px-3 py-2 text-sm" />
                    <button type="submit" className="py-2 px-4 bg-primary text-white font-bold rounded-lg text-sm">Apply</button>
                  </form>
                )}
                <input type="file" ref={fileInputRef} onChange={handleFileChange} accept="image/*,.pdf" className="hidden" />
                <button type="button" onClick={handleSendEvidence} disabled={uploading} className="py-2 px-4 bg-primary text-white font-bold rounded-lg text-sm disabled:opacity-60 flex items-center gap-2">
                  <span className="material-symbols-outlined text-base">upload</span>
                  {uploading ? 'Uploading...' : 'Send payment evidence'}
                </button>
              </>
            )}
            {dues?.pending_evidence && <p className="text-slate-500 text-xs">Payment evidence under review.</p>}
          </div>
        </div>

        {/* ── Team Roster ── */}
        {featuredMatchday?.my_group?.members?.length > 0 && (
          <div className="px-4 mb-6">
            <h3 className="text-lg font-bold mb-4">My Group</h3>
            <div className="bg-white dark:bg-slate-800/40 border border-slate-200 dark:border-primary/20 rounded-xl overflow-hidden">
              {featuredMatchday.my_group.members.map((m) => (
                <div key={m.id} className="flex items-center gap-3 p-3 border-b border-slate-100 dark:border-slate-700/50 last:border-0 hover:bg-primary/5 transition-colors">
                  <JerseyAvatar shortName={m.baller_name} number={m.jersey_number} status="in" />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-bold truncate text-slate-900 dark:text-slate-100">{m.baller_name}</p>
                    <p className="text-[10px] text-slate-500 uppercase">{m.first_name} {m.surname}</p>
                  </div>
                  <span className="bg-primary/10 text-primary text-[10px] px-2 py-0.5 rounded font-bold">IN</span>
                </div>
              ))}
              <div className="p-3">
                <button type="button" onClick={() => navigate('/matchday')} className="w-full py-2 text-xs font-bold text-slate-500 hover:text-primary transition-colors uppercase tracking-widest">
                  View All Matchdays
                </button>
              </div>
            </div>
          </div>
        )}
      </main>

    </div>
  );
}
