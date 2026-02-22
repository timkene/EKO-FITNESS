import { useEffect, useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { getPlayerAuth } from './Login';
import { getMemberDues, submitPaymentEvidence, applyWaiver, listMemberMatchdays, getMemberMatchday, voteMatchday, getMemberStats, getMemberTopFiveBallers } from '../api';
import JerseyAvatar from '../components/JerseyAvatar';
import './Dashboard.css';

function useCountdown(sundayDateStr) {
  const [left, setLeft] = useState({ days: 0, hours: 0, mins: 0 });
  useEffect(() => {
    if (!sundayDateStr) return;
    const kickoff = new Date(sundayDateStr + 'T18:00:00');
    const tick = () => {
      const now = new Date();
      let ms = kickoff - now;
      if (ms <= 0) {
        setLeft({ days: 0, hours: 0, mins: 0 });
        return;
      }
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
  const [dues, setDues] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState('');
  const [waiverDue, setWaiverDue] = useState('');
  const [waiverMsg, setWaiverMsg] = useState('');
  const [matchdays, setMatchdays] = useState([]);
  const [featuredMatchday, setFeaturedMatchday] = useState(null);
  const [voting, setVoting] = useState(false);
  const [memberStats, setMemberStats] = useState(null);
  const [topFiveBallers, setTopFiveBallers] = useState([]);
  const fileInputRef = useRef(null);

  useEffect(() => {
    if (!token) return;
    getMemberStats(token)
      .then((d) => setMemberStats(d))
      .catch(() => setMemberStats(null));
  }, [token]);

  useEffect(() => {
    if (!token) return;
    getMemberTopFiveBallers(token)
      .then((d) => setTopFiveBallers(d?.top_five || []))
      .catch(() => setTopFiveBallers([]));
  }, [token]);

  useEffect(() => {
    if (!token) return;
    getMemberDues(token)
      .then((data) => setDues(data))
      .catch(() => setDues({ status: 'owing', pending_evidence: false, year: new Date().getFullYear(), quarter: Math.ceil((new Date().getMonth() + 1) / 3) }));
  }, [token]);

  useEffect(() => {
    if (!token) return;
    listMemberMatchdays(token)
      .then((d) => setMatchdays(d.matchdays || []))
      .catch(() => setMatchdays([]));
  }, [token]);

  // Featured = first matchday that is voting_open or approved (not ended, not rejected)
  useEffect(() => {
    if (!token || matchdays.length === 0) {
      setFeaturedMatchday(null);
      return;
    }
    const next = matchdays.find((md) => !md.matchday_ended && md.status !== 'rejected' && (md.status === 'voting_open' || md.status === 'approved'));
    if (!next) {
      setFeaturedMatchday(null);
      return;
    }
    getMemberMatchday(next.id, token)
      .then(setFeaturedMatchday)
      .catch(() => setFeaturedMatchday(null));
  }, [token, matchdays]);

  const handleSendEvidence = () => fileInputRef.current?.click();

  const handleFileChange = async (e) => {
    const file = e.target.files?.[0];
    if (!file || !token) return;
    e.target.value = '';
    setUploadMsg('');
    setUploading(true);
    try {
      const data = await submitPaymentEvidence(file, token);
      setUploadMsg(data.message || 'Evidence submitted. Admin will review.');
      getMemberDues(token).then(setDues);
    } catch (err) {
      setUploadMsg(err.response?.data?.detail || 'Upload failed.');
    } finally {
      setUploading(false);
    }
  };

  const handleApplyWaiver = async (e) => {
    e.preventDefault();
    if (!waiverDue || !token) return;
    setWaiverMsg('');
    try {
      await applyWaiver(waiverDue, token);
      setWaiverMsg('Waiver applied. Pay by ' + waiverDue);
      getMemberDues(token).then(setDues);
    } catch (err) {
      setWaiverMsg(err.response?.data?.detail || 'Failed.');
    }
  };

  const handleVote = async () => {
    if (!featuredMatchday?.matchday?.id || !token) return;
    setVoting(true);
    try {
      await voteMatchday(featuredMatchday.matchday.id, token);
      getMemberMatchday(featuredMatchday.matchday.id, token).then(setFeaturedMatchday);
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

  return (
    <>
      <header className="min-h-14 md:h-20 border-b border-primary/10 px-4 md:px-8 flex items-center justify-between sticky top-0 bg-background-light/80 dark:bg-background-dark/80 backdrop-blur-md z-10 safe-area-inset-top">
        <h2 className="text-lg md:text-xl font-bold truncate">Team Dashboard</h2>
        <div className="flex items-center gap-4 md:gap-6">
          {md && (
            <div className="hidden sm:flex items-center gap-3 px-4 py-2 bg-primary/5 rounded-lg border border-primary/10">
              <span className="text-xs font-semibold text-primary uppercase tracking-widest">Next:</span>
              <span className="text-sm font-bold">Matchday {md.sunday_date}</span>
            </div>
          )}
        </div>
      </header>

      <div className="p-4 md:p-8 space-y-6 md:space-y-8 max-w-7xl mx-auto w-full overflow-x-hidden">
        {/* EKO TOP 5 BALLERS — in their faces, mobile-friendly */}
        <div className="bg-gradient-to-br from-amber-500/20 via-primary/10 to-slate-900/60 border-2 border-amber-500/30 rounded-xl md:rounded-2xl p-4 md:p-8 shadow-lg">
          <h2 className="text-lg md:text-2xl font-black text-center mb-1 md:mb-2 uppercase tracking-wider text-amber-400">
            EKO TOP 5 BALLERS
          </h2>
          <p className="text-slate-400 text-xs md:text-sm text-center mb-4 md:mb-6">Top 5 by rating — play to get here</p>
          {topFiveBallers.length > 0 ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-3 md:gap-6">
              {topFiveBallers.map((b, i) => (
                <div key={b.player_id} className="flex flex-col items-center text-center min-w-0">
                  <div className="relative flex justify-center">
                    <JerseyAvatar shortName={b.baller_name} number={b.jersey_number} size="lg" className="mx-auto shrink-0" />
                    <span className="absolute -top-0.5 -right-0.5 md:-top-1 md:-right-1 w-6 h-6 md:w-7 md:h-7 rounded-full bg-amber-500 text-slate-900 font-black text-xs md:text-sm flex items-center justify-center">{i + 1}</span>
                  </div>
                  <p className="font-bold text-slate-100 mt-1.5 md:mt-2 truncate w-full text-sm md:text-base">{b.baller_name}</p>
                  <p className="text-primary font-black text-base md:text-lg">{b.average_rating}</p>
                  <div className="flex gap-2 md:gap-3 mt-0.5 md:mt-1 text-[10px] md:text-xs text-slate-400">
                    <span title="Goals">G {b.goals}</span>
                    <span title="Assists">A {b.assists}</span>
                    <span title="Present">P {b.matchdays_present}</span>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-slate-400 text-center py-4 md:py-6 text-sm md:text-base px-2">Complete at least one matchday to see the EKO TOP 5 BALLERS.</p>
          )}
        </div>

        {/* Hero: Featured match + countdown (same UX as reference) */}
        {md && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="lg:col-span-2 relative group overflow-hidden rounded-xl h-64 flex flex-col justify-end p-8">
              <div
                className="absolute inset-0 bg-cover bg-center transition-transform duration-500 group-hover:scale-105"
                style={{ backgroundImage: "linear-gradient(to top, rgba(16, 34, 22, 0.95), rgba(16, 34, 22, 0.2)), url('https://images.unsplash.com/photo-1574629810360-7efbbe195018?w=800')" }}
                aria-hidden
              />
              <div className="relative z-0">
                <div className="flex items-center gap-3 mb-2">
                  <span className="bg-primary text-background-dark text-[10px] font-black px-2 py-0.5 rounded uppercase">Featured Match</span>
                  <span className="text-white/80 text-sm font-medium">Sunday, {md.sunday_date} • 18:00</span>
                </div>
                <h1 className="text-2xl sm:text-4xl font-black text-white leading-tight">Eko Football Matchday</h1>
                <p className="text-primary text-lg font-semibold tracking-wide flex items-center gap-2 mt-1">
                  <span className="material-symbols-outlined text-xl">location_on</span>
                  Eko Football — Matchday
                </p>
              </div>
            </div>
            {/* Countdown widget */}
            <div className="bg-primary/5 border border-primary/20 rounded-xl p-6 flex flex-col justify-center items-center text-center">
              <h3 className="text-xs font-bold uppercase tracking-[0.2em] text-primary mb-6">Kickoff In</h3>
              <div className="flex gap-4">
                <div className="flex flex-col items-center">
                  <span className="text-4xl font-black text-slate-900 dark:text-slate-100">{String(countdown.days).padStart(2, '0')}</span>
                  <span className="text-[10px] uppercase font-bold text-slate-600 dark:text-slate-500 mt-1">Days</span>
                </div>
                <span className="text-3xl font-black text-slate-400 dark:text-primary/30 mt-1">:</span>
                <div className="flex flex-col items-center">
                  <span className="text-4xl font-black text-slate-900 dark:text-slate-100">{String(countdown.hours).padStart(2, '0')}</span>
                  <span className="text-[10px] uppercase font-bold text-slate-600 dark:text-slate-500 mt-1">Hours</span>
                </div>
                <span className="text-3xl font-black text-slate-400 dark:text-primary/30 mt-1">:</span>
                <div className="flex flex-col items-center">
                  <span className="text-4xl font-black text-slate-900 dark:text-slate-100">{String(countdown.mins).padStart(2, '0')}</span>
                  <span className="text-[10px] uppercase font-bold text-slate-600 dark:text-slate-500 mt-1">Mins</span>
                </div>
              </div>
              <button
                type="button"
                onClick={() => navigate('/matchday')}
                className="mt-8 w-full min-h-[44px] py-3 bg-primary text-background-dark font-bold rounded-lg hover:shadow-[0_0_20px_rgba(10,194,71,0.3)] transition-all flex items-center justify-center gap-2 touch-manipulation"
              >
                <span className="material-symbols-outlined text-lg">confirmation_number</span>
                Match Details
              </button>
            </div>
          </div>
        )}

        {/* My stats: 8 metrics + global rank */}
        {memberStats?.stats && (
          <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 md:p-8">
            <h2 className="text-xl md:text-2xl font-bold mb-2">My stats</h2>
            {memberStats.global_rank != null && (
              <p className="text-primary font-bold mb-4">You are <span className="text-2xl">#{memberStats.global_rank}</span> on the global rating table (by average rating). <button type="button" onClick={() => navigate('/leaderboard')} className="text-sm underline hover:no-underline ml-2">View leaderboard</button></p>
            )}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
              <div className="rounded-lg bg-slate-800/50 p-3">
                <p className="text-slate-400 text-xs uppercase font-bold mb-1">Overall rating (avg)</p>
                <p className="text-2xl font-black text-primary">{memberStats.stats.average_rating}</p>
              </div>
              <div className="rounded-lg bg-slate-800/50 p-3">
                <p className="text-slate-400 text-xs uppercase font-bold mb-1">Goals</p>
                <p className="text-2xl font-black text-white">{memberStats.stats.goals}</p>
              </div>
              <div className="rounded-lg bg-slate-800/50 p-3">
                <p className="text-slate-400 text-xs uppercase font-bold mb-1">Assists</p>
                <p className="text-2xl font-black text-white">{memberStats.stats.assists}</p>
              </div>
              <div className="rounded-lg bg-slate-800/50 p-3">
                <p className="text-slate-400 text-xs uppercase font-bold mb-1">Clean sheets</p>
                <p className="text-2xl font-black text-white">{memberStats.stats.clean_sheets}</p>
              </div>
              <div className="rounded-lg bg-slate-800/50 p-3">
                <p className="text-slate-400 text-xs uppercase font-bold mb-1">Matchdays present</p>
                <p className="text-2xl font-black text-white">{memberStats.stats.matchdays_present}</p>
              </div>
              <div className="rounded-lg bg-slate-800/50 p-3">
                <p className="text-slate-400 text-xs uppercase font-bold mb-1">Yellow cards</p>
                <p className="text-2xl font-black text-amber-400">{memberStats.stats.yellow_cards}</p>
              </div>
              <div className="rounded-lg bg-slate-800/50 p-3">
                <p className="text-slate-400 text-xs uppercase font-bold mb-1">Red cards</p>
                <p className="text-2xl font-black text-red-400">{memberStats.stats.red_cards}</p>
              </div>
              <div className="rounded-lg bg-slate-800/50 p-3">
                <p className="text-slate-400 text-xs uppercase font-bold mb-1">Rating per matchday</p>
                <p className="text-sm font-bold text-slate-300">{memberStats.stats.matchday_ratings?.length ? `${memberStats.stats.matchday_ratings.length} matchdays` : '—'}</p>
              </div>
            </div>
            {memberStats.stats.matchday_ratings?.length > 0 && (
              <details className="mt-4">
                <summary className="text-sm text-slate-400 cursor-pointer hover:text-primary">Show rating per matchday</summary>
                <ul className="mt-2 space-y-1 text-sm">
                  {memberStats.stats.matchday_ratings.map((r) => (
                    <li key={r.matchday_id}><span className="text-slate-400">{r.sunday_date}</span> <strong className="text-primary">{r.rating}</strong></li>
                  ))}
                </ul>
              </details>
            )}
          </div>
        )}

        {/* Voting + Dues + Roster grid */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6 md:gap-8">
          <div className="lg:col-span-3 space-y-6">
            {/* Next Match Voting — same UX as reference (I'm In / Unavailable / Maybe) */}
            {hasVoting && (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-8">
                <div className="flex justify-between items-start mb-8">
                  <div>
                    <h2 className="text-2xl font-bold mb-1 text-black">Next Match Voting</h2>
                    <p className="text-black text-sm">Are you available for the upcoming match on <span className="text-black font-bold">{md.sunday_date}</span>?</p>
                  </div>
                  {featuredMatchday?.my_group?.members?.length > 0 && (
                    <div className="flex -space-x-2">
                      {featuredMatchday.my_group.members.slice(0, 3).map((m) => (
                        <JerseyAvatar key={m.id} shortName={m.baller_name} number={m.jersey_number} className="ring-2 ring-slate-900 rounded-lg" />
                      ))}
                      {featuredMatchday.my_group.members.length > 3 && (
                        <div className="size-8 rounded-lg border-2 border-slate-800 bg-slate-700 flex items-center justify-center text-[10px] font-bold text-black">+{featuredMatchday.my_group.members.length - 3}</div>
                      )}
                    </div>
                  )}
                </div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <button
                    type="button"
                    onClick={handleVote}
                    disabled={featuredMatchday?.voted || !featuredMatchday?.can_vote || voting}
                    className={`flex flex-col items-center gap-3 p-6 rounded-xl border-2 transition-all ${featuredMatchday?.voted ? 'border-primary bg-primary/10' : 'border-transparent bg-slate-800/50 hover:border-primary hover:bg-primary/20'}`}
                  >
                    <span className="material-symbols-outlined text-4xl text-primary font-bold">check_circle</span>
                    <span className="font-bold text-lg text-black">I'm In</span>
                    <span className="text-xs text-black uppercase font-black">
                      {featuredMatchday?.voted ? 'You voted' : voting ? 'Submitting...' : `${voteCount} Player${voteCount !== 1 ? 's' : ''} Confirmed`}
                    </span>
                  </button>
                  <button
                    type="button"
                    onClick={() => navigate('/matchday')}
                    className="flex flex-col items-center gap-3 p-6 rounded-xl border-2 border-transparent bg-slate-800/50 hover:border-red-500/50 transition-all group"
                  >
                    <span className="material-symbols-outlined text-4xl text-slate-600 group-hover:text-red-500 transition-colors">cancel</span>
                    <span className="font-bold text-lg text-black">Unavailable</span>
                    <span className="text-xs text-black uppercase font-black">Not this time</span>
                  </button>
                  <button
                    type="button"
                    onClick={() => navigate('/matchday')}
                    className="flex flex-col items-center gap-3 p-6 rounded-xl border-2 border-transparent bg-slate-800/50 hover:border-amber-500/50 transition-all group"
                  >
                    <span className="material-symbols-outlined text-4xl text-slate-600 group-hover:text-amber-500 transition-colors">help</span>
                    <span className="font-bold text-lg text-black">Maybe</span>
                    <span className="text-xs text-black uppercase font-black">Check Matchday</span>
                  </button>
                </div>
                <div className="mt-10 pt-10 border-t border-primary/5 flex flex-wrap gap-10">
                  <div>
                    <p className="text-[10px] uppercase font-black text-black mb-3 tracking-widest">Venue</p>
                    <div className="flex items-start gap-4">
                      <div className="size-20 rounded-lg bg-slate-800 flex items-center justify-center">
                        <span className="material-symbols-outlined text-3xl text-primary/60">stadium</span>
                      </div>
                      <div>
                        <p className="font-bold text-black">Eko Football</p>
                        <p className="text-sm text-black">Matchday {md.sunday_date}</p>
                        <button type="button" onClick={() => navigate('/matchday')} className="text-black text-xs font-bold hover:underline mt-1 block">Match Details</button>
                      </div>
                    </div>
                  </div>
                  <div className="flex-1 min-w-[200px]">
                    <p className="text-[10px] uppercase font-black text-black mb-3 tracking-widest">Squad Status</p>
                    <div className="flex gap-2 items-center">
                      <div className="flex-1 h-2 bg-slate-800 rounded-full overflow-hidden">
                        <div className="h-full bg-primary transition-all" style={{ width: `${Math.min(100, (voteCount / 18) * 100)}%` }} />
                      </div>
                      <span className="text-xs font-bold text-black">{voteCount} in</span>
                    </div>
                    <p className="text-xs text-black mt-3 italic">Confirm availability above. View full roster on Matchday.</p>
                  </div>
                </div>
                {!featuredMatchday?.can_vote && !featuredMatchday?.voted && (
                  <p className="text-black text-sm mt-4">Only paid or waiver members can vote.</p>
                )}
              </div>
            )}

            {/* Dues card */}
            <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 md:p-8">
              <h2 className="text-xl md:text-2xl font-bold mb-1">Quarterly dues</h2>
              <p className="text-slate-900 dark:text-slate-100 text-sm mb-4">{period}</p>
              <div className={`inline-block px-4 py-2 rounded-lg font-semibold ${dues?.status === 'paid' ? 'bg-primary/20 text-primary' : dues?.status === 'waiver' ? 'bg-amber-500/20 text-amber-400' : 'bg-red-500/20 text-red-400'}`}>
                {dues?.status === 'paid' ? 'Paid' : dues?.status === 'waiver' ? 'Waiver' : 'Owing'}
              </div>
              {dues?.status === 'waiver' && dues?.waiver_due_by && (
                <p className="text-slate-900 dark:text-slate-100 text-sm mt-3">Pay by {dues.waiver_due_by}</p>
              )}
              {dues?.status !== 'paid' && !dues?.pending_evidence && (
                <>
                  {dues?.status === 'owing' && (
                    <form onSubmit={handleApplyWaiver} className="mt-4 flex flex-wrap items-end gap-3">
                      <label className="block w-full text-sm text-slate-900 dark:text-slate-100">Apply for waiver (pay by date)</label>
                      <input type="date" value={waiverDue} onChange={(e) => setWaiverDue(e.target.value)} required className="rounded-lg bg-slate-800 border border-slate-600 px-3 py-2 text-sm" />
                      <button type="submit" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg hover:bg-primary/90">Apply for waiver</button>
                    </form>
                  )}
                  {waiverMsg && <p className="text-slate-900 dark:text-slate-100 text-sm mt-2">{waiverMsg}</p>}
                  <input type="file" ref={fileInputRef} onChange={handleFileChange} accept="image/*,.pdf" className="hidden" />
                  <button type="button" onClick={handleSendEvidence} disabled={uploading} className="mt-4 py-2 px-4 bg-primary text-background-dark font-bold rounded-lg hover:bg-primary/90 disabled:opacity-60">
                    {uploading ? 'Uploading...' : 'Send payment evidence'}
                  </button>
                </>
              )}
              {dues?.pending_evidence && <p className="text-slate-900 dark:text-slate-100 text-sm mt-3">Payment evidence under review.</p>}
              {uploadMsg && <p className="text-slate-900 dark:text-slate-100 text-sm mt-2">{uploadMsg}</p>}
            </div>
          </div>

          {/* Team Roster — jersey avatars (same UX as reference) */}
          <div className="lg:col-span-1 space-y-6">
            <div className="bg-slate-900/40 border border-primary/10 rounded-xl flex flex-col h-full">
              <div className="p-5 border-b border-primary/10 flex items-center justify-between">
                <h3 className="font-bold text-sm uppercase tracking-wider">Team Roster</h3>
              </div>
              <div className="p-2 flex-1 overflow-y-auto max-h-[500px] custom-scrollbar">
                {featuredMatchday?.my_group?.members?.length > 0 ? (
                  featuredMatchday.my_group.members.map((m) => (
                    <div key={m.id} className="flex items-center gap-3 p-3 rounded-lg hover:bg-primary/5 transition-colors group">
                      <JerseyAvatar shortName={m.baller_name} number={m.jersey_number} status="in" />
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-bold truncate group-hover:text-primary transition-colors text-slate-100">{m.baller_name}</p>
                        <p className="text-[10px] text-slate-500 uppercase font-black">{m.first_name} {m.surname}</p>
                      </div>
                      <span className="bg-primary/10 text-primary text-[10px] px-2 py-0.5 rounded font-bold">IN</span>
                    </div>
                  ))
                ) : (
                  <div className="p-4 text-center text-slate-500 text-sm">
                    {md?.groups_published ? 'You are not in a group for this matchday.' : 'Groups will appear here after the admin publishes them.'}
                    <button type="button" onClick={() => navigate('/matchday')} className="block mt-3 text-primary font-semibold hover:underline w-full">Open Matchday</button>
                  </div>
                )}
              </div>
              <div className="p-4 border-t border-primary/10">
                <button type="button" onClick={() => navigate('/matchday')} className="w-full py-2 text-xs font-bold text-slate-500 hover:text-primary transition-colors uppercase tracking-widest">
                  View All Matchdays
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
