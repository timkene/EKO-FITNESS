import { useEffect, useState } from 'react';
import { getPlayerAuth } from './Login';
import { getMemberLeaderboard, invalidateStatsCache } from '../api';
import { getSavedJersey } from './JerseyDesigner';
import { LeaderboardRowSkeleton } from '../components/Skeleton';

// Big jersey card for podium — scales up the JerseyAvatar concept
function PodiumJersey({ shortName, number, size = 'md' }) {
  const name = (shortName || '').trim().split(/\s+/)[0].slice(0, 8) || '—';
  const num = number != null ? String(number) : '?';
  const hasData = number != null && shortName;

  const dims = {
    lg: { box: 'w-28 h-36', name: 'text-sm', num: 'text-5xl' },
    md: { box: 'w-22 h-28', name: 'text-xs', num: 'text-4xl' },
    sm: { box: 'w-18 h-24', name: 'text-[10px]', num: 'text-3xl' },
  }[size] || { box: 'w-22 h-28', name: 'text-xs', num: 'text-4xl' };

  if (!hasData) {
    return (
      <div className={`${dims.box} rounded-xl bg-slate-800 border border-slate-700 flex flex-col items-center justify-center gap-1 p-2`} style={{ minWidth: size === 'lg' ? 112 : size === 'md' ? 88 : 72, minHeight: size === 'lg' ? 144 : size === 'md' ? 112 : 96 }}>
        <span className="material-symbols-outlined text-slate-600 text-2xl">checkroom</span>
        <p className="text-[8px] text-slate-600 text-center leading-tight">Set your jersey</p>
      </div>
    );
  }

  return (
    <div
      className={`rounded-xl bg-primary flex flex-col items-center justify-center text-black overflow-hidden`}
      style={{ minWidth: size === 'lg' ? 112 : size === 'md' ? 88 : 72, minHeight: size === 'lg' ? 144 : size === 'md' ? 112 : 96 }}
    >
      <span className={`${dims.name} font-bold leading-none uppercase tracking-widest mt-2`}>{name}</span>
      <span className={`${dims.num} font-black leading-none`}>{num}</span>
    </div>
  );
}

function PlayerModal({ player, onClose }) {
  if (!player) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/70 backdrop-blur-sm" />
      <div
        className="relative bg-[#0d1117] border border-[#1e2433] rounded-2xl p-6 w-full max-w-xs shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <button type="button" onClick={onClose} className="absolute top-3 right-3 text-slate-400 hover:text-white" aria-label="Close">
          <span className="material-symbols-outlined">close</span>
        </button>
        <div className="flex items-center gap-3 mb-5">
          <PodiumJersey shortName={player.baller_name} number={player.jersey_number} size="sm" />
          <div>
            <p className="font-black text-lg text-white">{player.baller_name}</p>
            {(player.star_rating ?? 0) >= 1 && (
              <p className="text-amber-400 text-sm">{'★'.repeat(Math.min(5, player.star_rating))}{'☆'.repeat(5 - Math.min(5, player.star_rating))}</p>
            )}
          </div>
        </div>
        <div className="grid grid-cols-2 gap-2 text-sm">
          {[
            { label: 'Avg Rating', value: player.average_rating, color: 'text-primary' },
            { label: 'Present',    value: player.matchdays_present, color: 'text-white' },
            { label: 'Goals',      value: player.goals,             color: 'text-white' },
            { label: 'Assists',    value: player.assists,           color: 'text-white' },
            { label: 'Clean Sheets', value: player.clean_sheets,   color: 'text-white' },
          ].map(({ label, value, color }) => (
            <div key={label} className="bg-white/5 border border-white/8 rounded-xl p-3">
              <p className="text-slate-400 text-[10px] uppercase font-bold mb-0.5">{label}</p>
              <p className={`font-black text-xl ${color}`}>{value}</p>
            </div>
          ))}
          <div className="bg-white/5 border border-white/8 rounded-xl p-3">
            <p className="text-slate-400 text-[10px] uppercase font-bold mb-0.5">Cards</p>
            <p className="font-black text-xl">
              <span className="text-amber-400">{player.yellow_cards}Y</span>
              <span className="text-slate-600 mx-1">/</span>
              <span className="text-red-400">{player.red_cards}R</span>
            </p>
          </div>
          {(player.motm_count ?? 0) > 0 && (
            <div className="col-span-2 bg-amber-500/10 border border-amber-500/25 rounded-xl p-3">
              <p className="text-amber-400 text-[10px] uppercase font-black mb-0.5">Man of the Match</p>
              <p className="text-amber-400 font-black text-xl">★ ×{player.motm_count}</p>
            </div>
          )}
          {(player.leagues_won ?? 0) > 0 && (
            <div className="col-span-2 bg-yellow-500/10 border border-yellow-500/25 rounded-xl p-3">
              <p className="text-yellow-400 text-[10px] uppercase font-black mb-0.5">🏆 Leagues Won</p>
              <p className="text-yellow-400 font-black text-xl">{player.leagues_won}</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

const PAGE_SIZE = 10;
const FILTERS = [
  { key: 'overall',      label: 'Overall',   sort: r => -r.average_rating },
  { key: 'goals',        label: 'Goals',     sort: r => -r.goals },
  { key: 'assists',      label: 'Assists',   sort: r => -r.assists },
  { key: 'present',      label: 'Present',   sort: r => -r.matchdays_present },
  { key: 'clean_sheets', label: 'C/Sheet',   sort: r => -r.clean_sheets },
  { key: 'motm',         label: 'MOTM',      sort: r => -(r.motm_count ?? 0) },
  { key: 'leagues_won',  label: '🏆 Leagues', sort: r => -(r.leagues_won ?? 0) },
];

export default function Leaderboard() {
  const { token, player } = getPlayerAuth();

  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [page, setPage] = useState(0);
  const [activeFilter, setActiveFilter] = useState('overall');

  const load = (bust = false) => {
    if (!token) return;
    if (bust) invalidateStatsCache();
    setLoading(true);
    setError(false);
    getMemberLeaderboard(token)
      .then((d) => { setData(d); setLoading(false); })
      .catch(() => { setError(true); setData(null); setLoading(false); });
  };

  // Initial load — always bust cache so we never show stale empty data
  useEffect(() => {
    load(true);
  }, [token]); // eslint-disable-line react-hooks/exhaustive-deps

  // Refresh when user returns to this tab
  useEffect(() => {
    if (!token) return;
    const refetch = () => {
      if (document.visibilityState === 'visible') {
        invalidateStatsCache();
        getMemberLeaderboard(token)
          .then(setData)
          .catch(() => {});
      }
    };
    document.addEventListener('visibilitychange', refetch);
    window.addEventListener('focus', refetch);
    return () => {
      document.removeEventListener('visibilitychange', refetch);
      window.removeEventListener('focus', refetch);
    };
  }, [token]);

  if (!player) return null;

  const savedJersey = getSavedJersey();
  const leaderboard = data?.leaderboard || [];

  // Top 3 always by overall rating
  const top3 = [...leaderboard].sort((a, b) => b.average_rating - a.average_rating).slice(0, 3);

  // Filtered + sorted table
  const filterDef = FILTERS.find(f => f.key === activeFilter) || FILTERS[0];
  const sorted = [...leaderboard].sort((a, b) => filterDef.sort(a) - filterDef.sort(b));
  const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
  const visibleRows = sorted.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const myRank = sorted.findIndex(r => r.player_id === player?.id);

  const handleFilter = (key) => { setActiveFilter(key); setPage(0); };

  return (
    <>
      <PlayerModal player={selectedPlayer} onClose={() => setSelectedPlayer(null)} />

      {/* Full dark container matching dashboard theme */}
      <div className="min-h-full bg-[#0d1117] relative overflow-hidden">

        {/* Stadium glow overlay */}
        <div className="absolute top-0 left-1/2 -translate-x-1/2 w-[700px] h-[350px] rounded-full pointer-events-none" style={{ background: 'radial-gradient(ellipse, rgba(10,194,71,0.12) 0%, transparent 70%)', filter: 'blur(40px)' }} />

        <div className="relative z-10 max-w-5xl mx-auto px-4 pb-10">

          {/* ── Header ── */}
          <header className="pt-8 pb-6 text-center">
            <h2 className="text-3xl font-black text-white tracking-tight uppercase italic drop-shadow-md">Club Leaderboard</h2>
            <div className="flex items-center justify-center gap-3 mt-1.5">
              <span className="h-px w-10 bg-primary/40" />
              <p className="text-primary font-bold text-[10px] tracking-[0.3em] uppercase">Season Rankings</p>
              <span className="h-px w-10 bg-primary/40" />
            </div>
          </header>

          {/* ── Top 3 Podium ── */}
          {loading ? (
            <div className="flex items-end justify-center gap-4 mb-10 px-4">
              {[96, 132, 72].map((h, i) => (
                <div key={i} className="flex-1 max-w-[220px]">
                  <div className="rounded-xl bg-white/5 animate-pulse mb-[-20px]" style={{ height: 160 }} />
                  <div className="rounded-t-xl bg-white/5 animate-pulse" style={{ height: h }} />
                </div>
              ))}
            </div>
          ) : top3.length > 0 && (
            <div className="flex items-end justify-center gap-3 mb-10 px-2">
              {/* #2 */}
              <div className="flex flex-col items-center flex-1 max-w-[200px]">
                <div className="relative mb-[-16px] z-10">
                  <PodiumJersey shortName={top3[1]?.baller_name} number={top3[1]?.jersey_number} size="md" />
                  <div className="absolute -top-2 -left-2 bg-slate-400 text-slate-900 font-black px-2 py-1 rounded-lg italic text-base shadow-lg">#2</div>
                </div>
                <div
                  className="w-full rounded-t-xl border-x border-t border-slate-500/30 flex flex-col items-center justify-end pb-3 pt-6 px-2 shadow-xl cursor-pointer hover:border-slate-400/50 transition-colors"
                  style={{ height: 96, background: 'linear-gradient(to top, rgba(148,163,184,0.15), rgba(148,163,184,0.04))' }}
                  onClick={() => top3[1] && setSelectedPlayer(top3[1])}
                >
                  <p className="text-sm font-bold text-white truncate w-full text-center">{top3[1]?.baller_name}</p>
                  <p className="text-primary font-black text-base italic">{top3[1]?.average_rating}</p>
                  {top3[1]?.player_id === player.id && savedJersey?.teamName ? (
                    <p className="text-[8px] text-primary/70 font-bold flex items-center gap-0.5 mt-0.5"><span className="material-symbols-outlined text-[9px]">checkroom</span>{savedJersey.teamName}</p>
                  ) : (
                    <p className="text-[8px] text-slate-600 flex items-center gap-0.5 mt-0.5"><span className="material-symbols-outlined text-[9px]">checkroom</span>No jersey</p>
                  )}
                </div>
              </div>

              {/* #1 — gold centerpiece */}
              <div className="flex flex-col items-center flex-1 max-w-[240px]">
                <div className="text-amber-400 text-xl text-center mb-1 drop-shadow-lg">👑</div>
                <div className="relative mb-[-16px] z-20">
                  <div style={{ transform: 'scale(1.08)', transformOrigin: 'bottom center' }}>
                    <div className="rounded-xl border-4 border-amber-500 shadow-[0_0_30px_rgba(245,158,11,0.35)] overflow-hidden">
                      <PodiumJersey shortName={top3[0]?.baller_name} number={top3[0]?.jersey_number} size="lg" />
                    </div>
                  </div>
                  <div className="absolute -top-3 -left-3 bg-amber-500 text-slate-900 font-black px-3 py-1.5 rounded-lg italic text-xl shadow-[0_0_16px_rgba(245,158,11,0.5)]">#1</div>
                  <div className="absolute inset-0 rounded-xl bg-amber-400/10 blur-xl -z-10" />
                </div>
                <div
                  className="w-full rounded-t-xl border-x border-t border-amber-500/40 flex flex-col items-center justify-end pb-4 pt-8 px-2 shadow-2xl cursor-pointer hover:border-amber-500/60 transition-colors"
                  style={{ height: 132, background: 'linear-gradient(to top, rgba(245,158,11,0.18), rgba(245,158,11,0.04))' }}
                  onClick={() => top3[0] && setSelectedPlayer(top3[0])}
                >
                  <p className="text-base font-black text-white italic truncate w-full text-center uppercase">{top3[0]?.baller_name}</p>
                  <p className="text-[9px] font-black text-amber-400 uppercase tracking-widest">Gold Rank</p>
                  <div className="mt-1 bg-amber-500 text-slate-900 font-black px-3 py-0.5 rounded-lg italic text-base shadow">{top3[0]?.average_rating} OVR</div>
                  {top3[0]?.player_id === player.id && savedJersey?.teamName ? (
                    <p className="text-[8px] text-primary/80 font-bold flex items-center gap-0.5 mt-1"><span className="material-symbols-outlined text-[9px]">checkroom</span>{savedJersey.teamName}</p>
                  ) : (
                    <p className="text-[8px] text-slate-600 flex items-center gap-0.5 mt-1"><span className="material-symbols-outlined text-[9px]">checkroom</span>No jersey</p>
                  )}
                </div>
              </div>

              {/* #3 */}
              <div className="flex flex-col items-center flex-1 max-w-[200px]">
                <div className="relative mb-[-16px] z-10">
                  <PodiumJersey shortName={top3[2]?.baller_name} number={top3[2]?.jersey_number} size="md" />
                  <div className="absolute -top-2 -right-2 bg-amber-700 text-white font-black px-2 py-1 rounded-lg italic text-base shadow-lg">#3</div>
                </div>
                <div
                  className="w-full rounded-t-xl border-x border-t border-amber-700/30 flex flex-col items-center justify-end pb-3 pt-6 px-2 shadow-xl cursor-pointer hover:border-amber-700/50 transition-colors"
                  style={{ height: 72, background: 'linear-gradient(to top, rgba(180,83,9,0.15), rgba(180,83,9,0.04))' }}
                  onClick={() => top3[2] && setSelectedPlayer(top3[2])}
                >
                  <p className="text-sm font-bold text-white truncate w-full text-center">{top3[2]?.baller_name}</p>
                  <p className="text-primary font-black text-base italic">{top3[2]?.average_rating}</p>
                  {top3[2]?.player_id === player.id && savedJersey?.teamName ? (
                    <p className="text-[8px] text-primary/70 font-bold flex items-center gap-0.5 mt-0.5"><span className="material-symbols-outlined text-[9px]">checkroom</span>{savedJersey.teamName}</p>
                  ) : (
                    <p className="text-[8px] text-slate-600 flex items-center gap-0.5 mt-0.5"><span className="material-symbols-outlined text-[9px]">checkroom</span>No jersey</p>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* ── Complete Squad Performance ── */}
          <div className="bg-slate-900/40 backdrop-blur-xl border border-[#1e2433] rounded-xl overflow-hidden shadow-2xl">
            {/* Table header + filter tabs */}
            <div className="px-5 py-4 border-b border-[#1e2433] flex flex-col sm:flex-row sm:items-center gap-3 bg-white/3">
              <h3 className="text-sm font-black text-white uppercase tracking-wider italic flex items-center gap-2">
                <span className="material-symbols-outlined text-primary text-base">groups</span>
                Complete Squad Performance
              </h3>
              <div className="flex sm:ml-auto bg-black/30 border border-[#1e2433] p-1 rounded-xl gap-0.5 flex-wrap">
                {FILTERS.map(f => (
                  <button
                    key={f.key}
                    onClick={() => handleFilter(f.key)}
                    className={`px-3 py-1.5 rounded-lg text-[10px] font-black uppercase tracking-wider transition-all ${
                      activeFilter === f.key ? 'bg-primary text-black shadow-sm shadow-primary/30' : 'text-slate-400 hover:text-white'
                    }`}
                  >
                    {f.label}
                  </button>
                ))}
              </div>
            </div>

            {/* My rank jump link */}
            {!loading && myRank >= 0 && Math.floor(myRank / PAGE_SIZE) !== page && (
              <div className="px-5 py-2 bg-primary/5 border-b border-[#1e2433]">
                <button onClick={() => setPage(Math.floor(myRank / PAGE_SIZE))} className="text-primary text-xs font-bold underline underline-offset-2">
                  Jump to my rank #{myRank + 1}
                </button>
              </div>
            )}

            <div className="overflow-x-auto">
              <table className="w-full text-xs min-w-[520px]">
                <thead>
                  <tr className="text-slate-500 border-b border-[#1e2433] bg-black/20 text-[10px] uppercase tracking-widest font-bold">
                    <th className="text-left py-3 px-4 w-10">#</th>
                    <th className="text-left py-3 px-4">Player</th>
                    <th className="text-center py-3 px-3">Stars</th>
                    <th className={`text-center py-3 px-3 ${activeFilter === 'overall' ? 'text-primary' : ''}`}>Avg</th>
                    <th className={`text-center py-3 px-3 ${activeFilter === 'goals' ? 'text-primary' : ''}`}>G</th>
                    <th className={`text-center py-3 px-3 ${activeFilter === 'assists' ? 'text-primary' : ''}`}>A</th>
                    <th className={`text-center py-3 px-3 ${activeFilter === 'clean_sheets' ? 'text-primary' : ''}`}>CS</th>
                    <th className={`text-center py-3 px-3 ${activeFilter === 'present' ? 'text-primary' : ''}`}>P</th>
                    <th className={`text-center py-3 px-3 ${activeFilter === 'motm' ? 'text-amber-400' : ''}`}>★</th>
                    <th className={`text-center py-3 px-3 ${activeFilter === 'leagues_won' ? 'text-yellow-400' : ''}`}>🏆</th>
                  </tr>
                </thead>
                <tbody>
                  {loading
                    ? Array.from({ length: 8 }, (_, i) => <LeaderboardRowSkeleton key={i} />)
                    : visibleRows.map((row, i) => {
                        const rank = page * PAGE_SIZE + i + 1;
                        const isMe = row.player_id === player.id;
                        return (
                          <tr
                            key={row.player_id}
                            onClick={() => setSelectedPlayer(row)}
                            className={`border-b border-[#1e2433]/60 last:border-0 cursor-pointer transition-colors group
                              ${isMe ? 'bg-primary/10' : 'hover:bg-white/3'}`}
                          >
                            <td className="py-3 px-4 font-black text-slate-500 italic">{rank}</td>
                            <td className="py-3 px-4">
                              <div className="flex items-center gap-2.5">
                                {/* Jersey mini-card — team color for current player, generic for others */}
                                <div
                                  className="w-9 h-9 rounded-lg flex flex-col items-center justify-center overflow-hidden shrink-0 relative"
                                  style={{ background: isMe && savedJersey?.teamColor ? savedJersey.teamColor : '#0ac247' }}
                                >
                                  <span className="text-[7px] font-bold leading-none uppercase text-black">
                                    {(row.baller_name || '').split(/\s+/)[0].slice(0, 6)}
                                  </span>
                                  <span className="text-base font-black leading-none text-black">{row.jersey_number ?? '?'}</span>
                                </div>
                                <div className="min-w-0">
                                  <span className={`font-bold truncate text-sm block ${isMe ? 'text-primary' : 'text-white group-hover:text-primary transition-colors'}`}>
                                    {row.baller_name}
                                  </span>
                                  {/* Club badge for current player; "set jersey" prompt for others */}
                                  {isMe && savedJersey?.teamName ? (
                                    <span className="text-[9px] font-bold text-primary/70 uppercase tracking-wide flex items-center gap-0.5">
                                      <span className="material-symbols-outlined text-[10px]">checkroom</span>
                                      {savedJersey.teamName}
                                    </span>
                                  ) : !isMe ? (
                                    <span className="text-[9px] text-slate-600 flex items-center gap-0.5">
                                      <span className="material-symbols-outlined text-[10px]">checkroom</span>
                                      No jersey set
                                    </span>
                                  ) : null}
                                </div>
                              </div>
                            </td>
                            <td className="py-3 px-3 text-center text-amber-400 text-[11px]">
                              {(row.star_rating ?? 0) >= 1
                                ? '★'.repeat(Math.min(5, row.star_rating))
                                : <span className="text-slate-600">—</span>}
                            </td>
                            <td className={`py-3 px-3 text-center font-black ${activeFilter === 'overall' ? 'text-primary italic text-sm' : 'text-slate-300'}`}>{row.average_rating}</td>
                            <td className={`py-3 px-3 text-center font-bold ${activeFilter === 'goals' ? 'text-primary' : 'text-slate-300'}`}>{row.goals}</td>
                            <td className={`py-3 px-3 text-center font-bold ${activeFilter === 'assists' ? 'text-primary' : 'text-slate-300'}`}>{row.assists}</td>
                            <td className={`py-3 px-3 text-center font-bold ${activeFilter === 'clean_sheets' ? 'text-primary' : 'text-slate-300'}`}>{row.clean_sheets}</td>
                            <td className={`py-3 px-3 text-center font-bold ${activeFilter === 'present' ? 'text-primary' : 'text-slate-300'}`}>{row.matchdays_present}</td>
                            <td className={`py-3 px-3 text-center font-bold ${activeFilter === 'motm' ? 'text-amber-400' : 'text-slate-500'}`}>
                              {(row.motm_count ?? 0) > 0 ? `★${row.motm_count}` : '—'}
                            </td>
                            <td className={`py-3 px-3 text-center font-bold ${activeFilter === 'leagues_won' ? 'text-yellow-400' : 'text-slate-500'}`}>
                              {(row.leagues_won ?? 0) > 0 ? `🏆${row.leagues_won}` : '—'}
                            </td>
                          </tr>
                        );
                      })
                  }
                </tbody>
              </table>
            </div>

            {!loading && error && (
              <div className="flex flex-col items-center py-10 gap-3">
                <p className="text-slate-400 text-sm">Couldn't load leaderboard data.</p>
                <button onClick={() => load(true)} className="px-4 py-2 bg-primary text-black text-xs font-black rounded-xl uppercase tracking-wider">
                  Try Again
                </button>
              </div>
            )}
            {!loading && !error && sorted.length === 0 && (
              <p className="text-slate-500 text-center py-10 text-sm">No ratings yet. Play matchdays to appear on the table.</p>
            )}

            {/* Pagination */}
            {!loading && totalPages > 1 && (
              <div className="flex items-center justify-between px-5 py-3 border-t border-[#1e2433] bg-black/20">
                <button
                  onClick={() => setPage(p => Math.max(0, p - 1))}
                  disabled={page === 0}
                  className="flex items-center gap-1 px-3 py-1.5 rounded-lg text-xs font-bold bg-white/5 border border-[#1e2433] text-white disabled:opacity-30 hover:bg-white/10 transition-colors"
                >
                  <span className="material-symbols-outlined text-sm">chevron_left</span> Prev
                </button>
                <span className="text-[11px] text-slate-500">
                  Page {page + 1} / {totalPages}
                  {myRank >= 0 && <span className="ml-2 text-primary font-bold">Your rank #{myRank + 1}</span>}
                </span>
                <button
                  onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                  disabled={page >= totalPages - 1}
                  className="flex items-center gap-1 px-3 py-1.5 rounded-lg text-xs font-bold bg-white/5 border border-[#1e2433] text-white disabled:opacity-30 hover:bg-white/10 transition-colors"
                >
                  Next <span className="material-symbols-outlined text-sm">chevron_right</span>
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
    </>
  );
}
