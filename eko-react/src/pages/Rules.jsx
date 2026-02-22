import { getPlayerAuth } from './Login';

const RATING_RULES = [
  { points: '+5', rule: 'All members that are present (marked present on the day)' },
  { points: '+2', rule: 'Any member that scores a goal' },
  { points: '+1', rule: 'Any member that assists a goal' },
  { points: '+5', rule: 'All members in the group that comes 1st at the end of matchday' },
  { points: '+3', rule: 'All members in the group that comes 2nd at the end of matchday' },
  { points: '+2', rule: 'All members in the group that comes 3rd at the end of matchday' },
  { points: '+1', rule: 'All members in the group that comes 4th at the end of matchday' },
  { points: '+1', rule: 'Per fixture: all members in a group that keeps a clean sheet in that fixture (no goals conceded in that game)' },
  { points: '+5', rule: 'Any member that scores a hat-trick (3 or more goals in one matchday)' },
  { points: '−5', rule: 'Per yellow card (each yellow card)' },
  { points: '−10', rule: 'Per red card (each red card)' },
];

export default function Rules() {
  const { player } = getPlayerAuth();

  if (!player) return null;

  return (
    <>
      <header className="min-h-14 md:h-20 border-b border-primary/10 px-4 md:px-8 flex items-center justify-between sticky top-0 bg-background-light/80 dark:bg-background-dark/80 backdrop-blur-md z-10 safe-area-inset-top">
        <h2 className="text-lg md:text-xl font-bold">Rating rules</h2>
      </header>

      <div className="p-4 md:p-8 max-w-3xl mx-auto w-full overflow-x-hidden">
        <p className="text-slate-400 dark:text-slate-300 text-sm mb-6">
          Your rating each matchday is the sum of the points below. Your <strong className="text-slate-100">average rating</strong> is used for the global leaderboard.
        </p>

        <div className="bg-slate-900/40 border border-primary/10 rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700 bg-slate-800/50">
                <th className="text-left py-4 px-4 font-bold text-primary w-20">Points</th>
                <th className="text-left py-4 px-4 font-bold text-slate-100">Rule</th>
              </tr>
            </thead>
            <tbody>
              {RATING_RULES.map((row, i) => (
                <tr key={i} className="border-b border-slate-700/50 last:border-0 hover:bg-primary/5">
                  <td className="py-3 px-4 font-bold text-primary text-lg tabular-nums">{row.points}</td>
                  <td className="py-3 px-4 text-slate-200">{row.rule}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <p className="text-slate-500 text-xs mt-6">
          Guests (&quot;Others&quot;) can score and assist but do not receive ratings. Only present members can be assigned goals, assists, or cards.
        </p>
      </div>
    </>
  );
}
