import { useEffect, useState } from 'react';
import { getPlayerAuth } from './Login';
import { getMemberLeaderboard } from '../api';

function StarDisplay({ count }) {
  if (count == null || count < 1) return <span className="text-slate-500 font-medium">0</span>;
  return (
    <span className="inline-flex items-center gap-0.5 text-amber-400" aria-label={`${count} star player`}>
      {'★'.repeat(Math.min(5, count))}{'☆'.repeat(5 - Math.min(5, count))}
    </span>
  );
}

function TopTable({ title, list, metricKey, metricLabel, playerId }) {
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
              className={`border-b border-slate-700/50 last:border-0 ${row.player_id === playerId ? 'bg-primary/10' : ''}`}
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

export default function Leaderboard() {
  const { token, player } = getPlayerAuth();
  const [data, setData] = useState(null);

  useEffect(() => {
    if (!token) return;
    getMemberLeaderboard(token)
      .then((d) => setData(d))
      .catch(() => setData(null));
  }, [token]);

  if (!player) return null;

  const leaderboard = data?.leaderboard || [];
  const topGoals = data?.top_goals || [];
  const topAssists = data?.top_assists || [];
  const topPresent = data?.top_present || [];
  const topCleanSheets = data?.top_clean_sheets || [];

  return (
    <>
      <header className="min-h-14 md:h-20 border-b border-primary/10 px-4 md:px-8 flex items-center justify-between sticky top-0 bg-background-light/80 dark:bg-background-dark/80 backdrop-blur-md z-10 safe-area-inset-top">
        <h2 className="text-lg md:text-xl font-bold truncate">Global rating table</h2>
      </header>

      <div className="p-4 md:p-8 max-w-5xl mx-auto w-full space-y-6 md:space-y-8 pb-safe">
        <p className="text-slate-400 text-xs md:text-sm mb-3 md:mb-4">Ranked by average rating. Your row is highlighted.</p>
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
              {leaderboard.map((row, i) => (
                <tr
                  key={row.player_id}
                  className={`border-b border-slate-700/50 ${row.player_id === player.id ? 'bg-primary/10' : 'hover:bg-primary/5'}`}
                >
                  <td className="py-2 px-1.5 md:px-2 font-bold text-slate-500">{i + 1}</td>
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
              ))}
            </tbody>
          </table>
        </div>
        {leaderboard.length === 0 && <p className="text-slate-500 text-center py-8">No ratings yet. Play matchdays to appear on the table.</p>}

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 md:gap-4 pt-4">
          <TopTable title="Top goals overall" list={topGoals} metricKey="goals" metricLabel="Goals" playerId={player.id} />
          <TopTable title="Top assists overall" list={topAssists} metricKey="assists" metricLabel="Assists" playerId={player.id} />
          <TopTable title="Top present overall" list={topPresent} metricKey="matchdays_present" metricLabel="Present" playerId={player.id} />
          <TopTable title="Top clean sheets overall" list={topCleanSheets} metricKey="clean_sheets" metricLabel="Clean sheets" playerId={player.id} />
        </div>
      </div>
    </>
  );
}
