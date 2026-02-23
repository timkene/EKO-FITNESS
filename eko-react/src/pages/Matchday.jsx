import { useEffect, useState } from 'react';
import { getPlayerAuth } from './Login';
import { listMemberMatchdays, getMemberMatchday, voteMatchday } from '../api';
import JerseyAvatar from '../components/JerseyAvatar';
import { useToast } from '../components/Toast';
import { MatchdayListSkeleton } from '../components/Skeleton';
import './Matchday.css';

const PAGE_SIZE = 10;

export default function Matchday() {
  const { token, player } = getPlayerAuth();
  const toast = useToast();
  const [matchdays, setMatchdays] = useState([]);
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);
  const [selectedId, setSelectedId] = useState(null);
  const [detail, setDetail] = useState(null);
  const [loading, setLoading] = useState(false);
  const [voting, setVoting] = useState(false);

  useEffect(() => {
    if (!token) return;
    setLoading(true);
    listMemberMatchdays(token)
      .then((d) => setMatchdays(d.matchdays || []))
      .catch(() => {
        toast('Failed to load matchdays.', 'error');
        setMatchdays([]);
      })
      .finally(() => setLoading(false));
  }, [token]);

  useEffect(() => {
    if (!token || selectedId == null) {
      setDetail(null);
      return;
    }
    setLoading(true);
    getMemberMatchday(selectedId, token)
      .then(setDetail)
      .catch(() => {
        toast('Failed to load matchday.', 'error');
        setDetail(null);
      })
      .finally(() => setLoading(false));
  }, [token, selectedId]);

  const handleVote = async () => {
    if (!selectedId || !token) return;
    setVoting(true);
    try {
      await voteMatchday(selectedId, token);
      toast('Vote recorded!', 'success');
      getMemberMatchday(selectedId, token).then(setDetail);
    } catch (e) {
      toast(e.response?.data?.detail || 'Vote failed.', 'error');
    } finally {
      setVoting(false);
    }
  };

  if (!player) return null;

  const visibleMatchdays = matchdays.slice(0, visibleCount);
  const hasMore = matchdays.length > visibleCount;

  return (
    <>
      <header className="min-h-14 md:h-20 border-b border-primary/10 px-4 md:px-8 flex items-center justify-between sticky top-0 bg-background-light/80 dark:bg-background-dark/80 backdrop-blur-md z-10 safe-area-inset-top">
        <h2 className="text-lg md:text-xl font-bold">Matchday</h2>
      </header>

      <div className="p-4 md:p-8 space-y-6 md:space-y-8 max-w-7xl mx-auto w-full overflow-x-hidden">

        {loading && !detail && selectedId == null ? (
          <MatchdayListSkeleton />
        ) : selectedId == null ? (
          <>
            <p className="text-slate-400 text-sm">View past matchdays or the current one when admin has created it.</p>
            <ul className="space-y-3">
              {visibleMatchdays.map((md) => (
                <li key={md.id} className="flex flex-wrap items-center justify-between gap-3 p-4 rounded-xl bg-slate-900/40 border border-primary/10 hover:border-primary/20 transition-colors">
                  <div>
                    <strong className="text-white">{md.sunday_date}</strong>
                    <span className="text-slate-400 text-sm ml-2">{md.status}{md.matchday_ended ? ' (ended)' : ''}</span>
                  </div>
                  <button
                    type="button"
                    onClick={() => setSelectedId(md.id)}
                    className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg hover:shadow-[0_0_16px_rgba(10,194,71,0.3)] transition-all"
                  >
                    View
                  </button>
                </li>
              ))}
            </ul>
            {hasMore && (
              <button
                type="button"
                onClick={() => setVisibleCount((n) => n + PAGE_SIZE)}
                className="w-full py-2 text-sm text-slate-400 hover:text-primary transition-colors border border-slate-700 rounded-xl hover:border-primary/30"
              >
                Show more ({matchdays.length - visibleCount} remaining)
              </button>
            )}
            {matchdays.length === 0 && (
              <p className="text-slate-500 text-center py-12">No matchdays yet. Admin will create one to open voting.</p>
            )}
          </>
        ) : detail ? (
          <div className="space-y-6">
            <div className="flex flex-wrap items-center gap-3">
              <button
                type="button"
                onClick={() => { setSelectedId(null); setDetail(null); }}
                className="flex items-center gap-2 py-2 px-3 rounded-lg border border-slate-600 text-slate-400 hover:bg-slate-800 hover:text-white transition-colors text-sm"
              >
                <span className="material-symbols-outlined text-lg">arrow_back</span>
                Back to list
              </button>
              <button
                type="button"
                onClick={() => getMemberMatchday(selectedId, token).then(setDetail)}
                disabled={loading}
                className="flex items-center gap-2 py-2 px-3 rounded-lg border border-primary/30 text-primary hover:bg-primary/10 transition-colors text-sm disabled:opacity-50"
              >
                <span className="material-symbols-outlined text-lg">refresh</span>
                Refresh
              </button>
            </div>

            <h2 className="text-2xl font-bold">Matchday – {detail.matchday?.sunday_date}</h2>
            <p className="text-slate-400 text-sm">Status: {detail.matchday?.status}</p>

            {detail.matchday?.status === 'voting_open' && (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                <h3 className="font-bold mb-3">Vote</h3>
                {detail.voted ? (
                  <p className="text-primary font-medium">You have voted.</p>
                ) : detail.can_vote ? (
                  <button
                    type="button"
                    onClick={handleVote}
                    disabled={voting}
                    className="py-3 px-6 bg-primary text-background-dark font-bold rounded-lg hover:shadow-[0_0_16px_rgba(10,194,71,0.3)] transition-all disabled:opacity-60"
                  >
                    {voting ? 'Submitting...' : 'Vote for matchday'}
                  </button>
                ) : (
                  <p className="text-slate-400 text-sm">Only paid or waiver members can vote.</p>
                )}
              </div>
            )}

            {detail.matchday?.status === 'rejected' && (
              <p className="text-red-400 font-medium">This matchday was cancelled.</p>
            )}

            {/* All groups */}
            {detail.matchday?.status === 'approved' && detail.all_groups?.length > 0 && (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                <h3 className="font-bold mb-4">All groups</h3>
                <p className="text-slate-200 text-sm mb-4">Baller names only. Present = played on the day.</p>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                  {detail.all_groups.map((g) => (
                    <div key={g.group_index} className="rounded-lg border border-slate-700 p-3">
                      <h4 className="font-semibold text-slate-100 mb-2">Group {g.group_index}</h4>
                      <ul className="space-y-1.5">
                        {g.members?.map((m, i) => (
                          <li key={i} className="flex items-center justify-between text-sm">
                            <span className="font-medium">{m.baller_name}</span>
                            {m.present !== undefined && (
                              <span className={`text-[10px] uppercase font-bold px-2 py-0.5 rounded ${m.present ? 'bg-primary/20 text-slate-100' : 'bg-slate-700 text-slate-300'}`}>
                                {m.present ? 'Present' : 'Absent'}
                              </span>
                            )}
                          </li>
                        ))}
                      </ul>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {detail.matchday?.status === 'approved' && detail.my_group && !detail.all_groups?.length && (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                <h3 className="font-bold mb-4">Your group (Group {detail.my_group.group_index})</h3>
                <ul className="space-y-2">
                  {detail.my_group.members.map((m) => (
                    <li key={m.id} className="flex items-center gap-3 p-2 rounded-lg hover:bg-primary/5">
                      <JerseyAvatar shortName={m.baller_name} number={m.jersey_number} />
                      <span className="font-medium text-slate-100">{m.baller_name}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Fixtures */}
            {detail.fixtures?.length > 0 && (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                <h3 className="font-bold mb-4">Fixtures</h3>
                <ul className="space-y-4">
                  {detail.fixtures.map((f) => (
                    <li key={f.id} className="border-b border-slate-700/50 pb-4 last:border-0 last:pb-0">
                      <div className="flex items-center justify-between flex-wrap gap-2">
                        <span className="font-medium">Group {f.group_a_index} vs Group {f.group_b_index}</span>
                        <span className="text-primary font-bold text-lg">{f.home_goals} – {f.away_goals}</span>
                        <span className="text-slate-500 text-sm">{f.status}</span>
                      </div>
                      {f.goals?.length > 0 && (
                        <ul className="mt-2 ml-2 space-y-1 text-sm text-slate-200">
                          {f.goals.map((g, i) => (
                            <li key={i}>
                              {g.minute != null ? `${g.minute}' ` : ''}
                              <span className="text-white font-medium">{g.scorer_name}</span>
                              {g.assister_name ? <span> (assist: {g.assister_name})</span> : ''}
                            </li>
                          ))}
                        </ul>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* League table */}
            {detail.table?.length > 0 && (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 overflow-x-auto">
                <h3 className="font-bold mb-2">League table</h3>
                <p className="text-slate-200 text-xs mb-4">P=Played, W=Won, D=Drawn, L=Lost, GF=Goals for, GA=Goals against. Pts: 3 win, 1 draw.</p>
                <table className="w-full min-w-[400px] text-sm">
                  <thead>
                    <tr className="text-slate-200 border-b border-slate-700">
                      <th className="text-left py-2 px-2 font-semibold">Group</th>
                      <th className="text-center py-2 px-2 font-semibold">P</th>
                      <th className="text-center py-2 px-2 font-semibold">W</th>
                      <th className="text-center py-2 px-2 font-semibold">D</th>
                      <th className="text-center py-2 px-2 font-semibold">L</th>
                      <th className="text-center py-2 px-2 font-semibold">GF</th>
                      <th className="text-center py-2 px-2 font-semibold">GA</th>
                      <th className="text-center py-2 px-2 font-semibold">Pts</th>
                    </tr>
                  </thead>
                  <tbody>
                    {detail.table.map((row) => (
                      <tr key={row.group_id} className="border-b border-slate-700/50 hover:bg-primary/5">
                        <td className="py-2 px-2 font-medium text-slate-100">Group {row.group_index}</td>
                        <td className="py-2 px-2 text-center text-slate-100">{row.played}</td>
                        <td className="py-2 px-2 text-center text-slate-100">{row.won}</td>
                        <td className="py-2 px-2 text-center text-slate-100">{row.drawn}</td>
                        <td className="py-2 px-2 text-center text-slate-100">{row.lost}</td>
                        <td className="py-2 px-2 text-center text-slate-100">{row.goals_for}</td>
                        <td className="py-2 px-2 text-center text-slate-100">{row.goals_against}</td>
                        <td className="py-2 px-2 text-center font-bold text-slate-100">{row.points}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Top scorers */}
            {detail.top_scorers?.length > 0 && (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 overflow-x-auto">
                <h3 className="font-bold mb-4">Top goalscorers</h3>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-slate-200 border-b border-slate-700">
                      <th className="text-left py-2 px-2 font-semibold">#</th>
                      <th className="text-left py-2 px-2 font-semibold">Baller</th>
                      <th className="text-center py-2 px-2 font-semibold">Goals</th>
                    </tr>
                  </thead>
                  <tbody>
                    {detail.top_scorers.map((row, i) => (
                      <tr key={i} className="border-b border-slate-700/50 hover:bg-primary/5">
                        <td className="py-2 px-2 text-slate-300">{i + 1}</td>
                        <td className="py-2 px-2 font-medium text-slate-100">{row.baller_name}</td>
                        <td className="py-2 px-2 text-center font-bold text-slate-100">{row.goals}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Top assists */}
            {detail.top_assists?.length > 0 && (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 overflow-x-auto">
                <h3 className="font-bold mb-4">Top assists</h3>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-slate-200 border-b border-slate-700">
                      <th className="text-left py-2 px-2 font-semibold">#</th>
                      <th className="text-left py-2 px-2 font-semibold">Baller</th>
                      <th className="text-center py-2 px-2 font-semibold">Assists</th>
                    </tr>
                  </thead>
                  <tbody>
                    {detail.top_assists.map((row, i) => (
                      <tr key={i} className="border-b border-slate-700/50 hover:bg-primary/5">
                        <td className="py-2 px-2 text-slate-300">{i + 1}</td>
                        <td className="py-2 px-2 font-medium text-slate-100">{row.baller_name}</td>
                        <td className="py-2 px-2 text-center font-bold text-slate-100">{row.assists}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Top ratings */}
            {detail.top_ratings?.length > 0 && (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 overflow-x-auto">
                <h3 className="font-bold mb-4">Top player ratings</h3>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-slate-200 border-b border-slate-700">
                      <th className="text-left py-2 px-2 font-semibold">#</th>
                      <th className="text-left py-2 px-2 font-semibold">Baller</th>
                      <th className="text-center py-2 px-2 font-semibold">Rating</th>
                    </tr>
                  </thead>
                  <tbody>
                    {detail.top_ratings.map((row, i) => (
                      <tr key={i} className="border-b border-slate-700/50 hover:bg-primary/5">
                        <td className="py-2 px-2 text-slate-300">{i + 1}</td>
                        <td className="py-2 px-2 font-medium text-slate-100">{row.baller_name}</td>
                        <td className="py-2 px-2 text-center font-bold text-slate-100">{row.rating}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        ) : loading ? (
          <MatchdayListSkeleton />
        ) : (
          <p className="text-slate-500">Could not load matchday.</p>
        )}
      </div>
    </>
  );
}
