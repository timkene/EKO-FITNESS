import { useEffect, useState } from 'react';
import { getPlayerAuth } from './Login';
import { getMemberLeaderboard } from '../api';
import { useToast } from '../components/Toast';
import { LeaderboardRowSkeleton } from '../components/Skeleton';

function StarDisplay({ count }) {
  if (count == null || count < 1) return <span className="text-slate-500 font-medium">0</span>;
  return (
    <span className="inline-flex items-center gap-0.5 text-amber-400" aria-label={`${count} star player`}>
      {'★'.repeat(Math.min(5, count))}{'☆'.repeat(5 - Math.min(5, count))}
    </span>
  );
}

function TopTable({ title, list, metricKey, metricLabel, playerId, onPlayerClick }) {
  const rows = list || [];
  return (
    <div className="bg-slate-900/40 border border-primary/10 rounded-xl overflow-hidden">
      <h3 className="text-xs md:text-sm font-bold text-primary px-3 md:px-4 py-2 md:py-3 border-b border-slate-700">{title}</h3>
      <table className="w-full text-xs md:text-sm">
        <thead>
          <tr className="text-slate-400 border-b border-slate-700">
            <th className="text-left py-1.5 md:py-2 px-2 md:px-3 w-8">#</th>
            <th className="text-left py-1.5 md:py-2 px-2 md:px-3">Baller</th>
            <th className="text-center py-1.5 md:py-2 px-2 md:px-3 font-bold text-primary">{metricLabel}</th>
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 10).map((row, i) => (
            <tr
              key={row.player_id}
              onClick={() => onPlayerClick && onPlayerClick(row)}
              className={`border-b border-slate-700/50 last:border-0 cursor-pointer transition-colors
                ${row.player_id === playerId ? 'bg-primary/10' : 'hover:bg-primary/5'}`}
            >
              <td className="py-1.5 md:py-2 px-2 md:px-3 font-bold text-slate-500">{i + 1}</td>
              <td className="py-1.5 md:py-2 px-2 md:px-3 font-medium truncate max-w-[100px] md:max-w-none">{row.baller_name}</td>
              <td className="py-1.5 md:py-2 px-2 md:px-3 text-center font-bold">{row[metricKey]}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length === 0 && <p className="text-slate-500 text-center py-3 md:py-4 text-xs md:text-sm">No data yet.</p>}
    </div>
  );
}

function PlayerModal({ player, onClose }) {
  if (!player) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />
      <div
        className="relative bg-slate-900 border border-primary/20 rounded-2xl p-6 w-full max-w-xs shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          type="button"
          onClick={onClose}
          className="absolute top-3 right-3 text-slate-400 hover:text-slate-100"
          aria-label="Close"
        >
          <span className="material-symbols-outlined">close</span>
        </button>
        <div className="flex items-center gap-3 mb-4">
          <div className="bg-primary/20 border border-primary/30 rounded-xl w-12 h-12 flex items-center justify-center shrink-0">
            <span className="text-primary font-black text-lg">#{player.jersey_number ?? '?'}</span>
          </div>
          <div>
            <p className="font-black text-lg text-slate-100">{player.baller_name}</p>
            <StarDisplay count={player.star_rating} />
          </div>
        </div>
        <div className="grid grid-cols-2 gap-2 text-sm">
          <div className="bg-slate-800/60 rounded-lg p-2.5">
            <p className="text-slate-400 text-xs uppercase font-bold mb-0.5">Avg rating</p>
            <p className="text-primary font-black text-lg">{player.average_rating}</p>
          </div>
          <div className="bg-slate-800/60 rounded-lg p-2.5">
            <p className="text-slate-400 text-xs uppercase font-bold mb-0.5">Present</p>
            <p className="text-white font-black text-lg">{player.matchdays_present}</p>
          </div>
          <div className="bg-slate-800/60 rounded-lg p-2.5">
            <p className="text-slate-400 text-xs uppercase font-bold mb-0.5">Goals</p>
            <p className="text-white font-black text-lg">{player.goals}</p>
          </div>
          <div className="bg-slate-800/60 rounded-lg p-2.5">
            <p className="text-slate-400 text-xs uppercase font-bold mb-0.5">Assists</p>
            <p className="text-white font-black text-lg">{player.assists}</p>
          </div>
          <div className="bg-slate-800/60 rounded-lg p-2.5">
            <p className="text-slate-400 text-xs uppercase font-bold mb-0.5">Clean sheets</p>
            <p className="text-white font-black text-lg">{player.clean_sheets}</p>
          </div>
          <div className="bg-slate-800/60 rounded-lg p-2.5">
            <p className="text-slate-400 text-xs uppercase font-bold mb-0.5">Cards</p>
            <p className="font-black text-lg">
              <span className="text-amber-400">{player.yellow_cards}Y</span>
              <span className="text-slate-500 mx-1">/</span>
              <span className="text-red-400">{player.red_cards}R</span>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

const PAGE_SIZE = 10;

export default function Leaderboard() {
  const { token, player } = getPlayerAuth();
  const toast = useToast();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [page, setPage] = useState(0);

  useEffect(() => {
    if (!token) return;
    setLoading(true);
    getMemberLeaderboard(token)
      .then((d) => { setData(d); setLoading(false); })
      .catch(() => {
        toast('Failed to load leaderboard.', 'error');
        setData(null);
        setLoading(false);
      });
  }, [token]);

  if (!player) return null;

  const leaderboard = data?.leaderboard || [];
  const topGoals = data?.top_goals || [];
  const topAssists = data?.top_assists || [];
  const topPresent = data?.top_present || [];
  const topCleanSheets = data?.top_clean_sheets || [];

  const totalPages = Math.ceil(leaderboard.length / PAGE_SIZE);
  const visibleRows = leaderboard.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const myRank = leaderboard.findIndex((r) => r.player_id === player?.id);
  const myPage = myRank >= 0 ? Math.floor(myRank / PAGE_SIZE) : -1;

  return (
    <>
      <PlayerModal player={selectedPlayer} onClose={() => setSelectedPlayer(null)} />

      <header className="min-h-14 md:h-20 border-b border-primary/10 px-4 md:px-8 flex items-center justify-between sticky top-0 bg-background-light/80 dark:bg-background-dark/80 backdrop-blur-md z-10 safe-area-inset-top">
        <h2 className="text-lg md:text-xl font-bold truncate">Global rating table</h2>
        {!loading && myRank >= 0 && myPage !== page && (
          <button
            onClick={() => setPage(myPage)}
            className="text-xs text-primary underline underline-offset-2 shrink-0 ml-2"
          >
            My rank #{myRank + 1}
          </button>
        )}
      </header>

      <div className="p-4 md:p-8 max-w-5xl mx-auto w-full space-y-6 md:space-y-8 pb-safe">
        <p className="text-slate-400 text-xs md:text-sm mb-3 md:mb-4">Ranked by average rating. Your row is highlighted. Tap a player to view their profile.</p>
        <div className="overflow-x-auto -mx-4 px-4 md:mx-0 md:px-0">
          <table className="w-full text-xs md:text-sm min-w-[580px]">
            <thead>
              <tr className="text-slate-400 border-b border-slate-700">
                <th className="text-left py-2 md:py-3 px-1.5 md:px-2">#</th>
                <th className="text-left py-2 md:py-3 px-1.5 md:px-2">Baller</th>
                <th className="text-center py-2 md:py-3 px-1.5 md:px-2">Stars</th>
                <th className="text-center py-2 md:py-3 px-1.5 md:px-2 font-bold text-primary">Avg</th>
                <th className="text-center py-2 md:py-3 px-1.5 md:px-2">G</th>
                <th className="text-center py-2 md:py-3 px-1.5 md:px-2">A</th>
                <th className="text-center py-2 md:py-3 px-1.5 md:px-2">CS</th>
                <th className="text-center py-2 md:py-3 px-1.5 md:px-2">P</th>
                <th className="text-center py-2 md:py-3 px-1.5 md:px-2">Y</th>
                <th className="text-center py-2 md:py-3 px-1.5 md:px-2">R</th>
              </tr>
            </thead>
            <tbody>
              {loading
                ? Array.from({ length: 8 }, (_, i) => <LeaderboardRowSkeleton key={i} />)
                : visibleRows.map((row, i) => (
                  <tr
                    key={row.player_id}
                    onClick={() => setSelectedPlayer(row)}
                    className={`border-b border-slate-700/50 cursor-pointer transition-colors
                      ${row.player_id === player.id ? 'bg-primary/10' : 'hover:bg-primary/5'}`}
                  >
                    <td className="py-2 px-1.5 md:px-2 font-bold text-slate-500">{page * PAGE_SIZE + i + 1}</td>
                    <td className="py-2 px-1.5 md:px-2 font-medium min-w-[72px]">{row.baller_name}</td>
                    <td className="py-2 px-1.5 md:px-2 text-center"><StarDisplay count={row.star_rating} /></td>
                    <td className="py-2 px-1.5 md:px-2 text-center font-bold text-primary">{row.average_rating}</td>
                    <td className="py-2 px-1.5 md:px-2 text-center">{row.goals}</td>
                    <td className="py-2 px-1.5 md:px-2 text-center">{row.assists}</td>
                    <td className="py-2 px-1.5 md:px-2 text-center">{row.clean_sheets}</td>
                    <td className="py-2 px-1.5 md:px-2 text-center">{row.matchdays_present}</td>
                    <td className="py-2 px-1.5 md:px-2 text-center text-amber-400">{row.yellow_cards}</td>
                    <td className="py-2 px-1.5 md:px-2 text-center text-red-400">{row.red_cards}</td>
                  </tr>
                ))
              }
            </tbody>
          </table>
        </div>
        {!loading && leaderboard.length === 0 && <p className="text-slate-500 text-center py-8">No ratings yet. Play matchdays to appear on the table.</p>}
        {!loading && totalPages > 1 && (
          <div className="flex items-center justify-between pt-2">
            <button
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
              className="flex items-center gap-1 px-3 py-1.5 rounded-lg text-sm font-semibold bg-slate-800 border border-slate-700 disabled:opacity-30 hover:bg-slate-700 transition-colors"
            >
              <span className="material-symbols-outlined text-base">chevron_left</span> Prev
            </button>
            <span className="text-xs text-slate-400">
              {page + 1} / {totalPages}
              {myRank >= 0 && <span className="ml-2 text-primary font-semibold">Your rank #{myRank + 1}</span>}
            </span>
            <button
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              className="flex items-center gap-1 px-3 py-1.5 rounded-lg text-sm font-semibold bg-slate-800 border border-slate-700 disabled:opacity-30 hover:bg-slate-700 transition-colors"
            >
              Next <span className="material-symbols-outlined text-base">chevron_right</span>
            </button>
          </div>
        )}

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 md:gap-4 pt-4">
          <TopTable title="Top goals overall" list={topGoals} metricKey="goals" metricLabel="Goals" playerId={player.id} onPlayerClick={setSelectedPlayer} />
          <TopTable title="Top assists overall" list={topAssists} metricKey="assists" metricLabel="Assists" playerId={player.id} onPlayerClick={setSelectedPlayer} />
          <TopTable title="Top present overall" list={topPresent} metricKey="matchdays_present" metricLabel="Present" playerId={player.id} onPlayerClick={setSelectedPlayer} />
          <TopTable title="Top clean sheets overall" list={topCleanSheets} metricKey="clean_sheets" metricLabel="Clean sheets" playerId={player.id} onPlayerClick={setSelectedPlayer} />
        </div>
      </div>
    </>
  );
}
