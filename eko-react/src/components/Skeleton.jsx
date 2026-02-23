/** Reusable skeleton loading components */

export function SkeletonBox({ className = '' }) {
  return (
    <div className={`animate-pulse bg-slate-700/50 rounded-lg ${className}`} />
  );
}

export function SkeletonText({ className = '' }) {
  return (
    <div className={`animate-pulse bg-slate-700/50 rounded h-4 ${className}`} />
  );
}

export function StatCardSkeleton() {
  return (
    <div className="rounded-lg bg-slate-800/50 p-3 animate-pulse">
      <div className="h-3 w-24 bg-slate-700/50 rounded mb-2" />
      <div className="h-8 w-16 bg-slate-700/50 rounded" />
    </div>
  );
}

export function LeaderboardRowSkeleton() {
  return (
    <tr className="border-b border-slate-700/50">
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-4 bg-slate-700/50 rounded animate-pulse" /></td>
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-24 bg-slate-700/50 rounded animate-pulse" /></td>
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-16 bg-slate-700/50 rounded animate-pulse mx-auto" /></td>
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-10 bg-slate-700/50 rounded animate-pulse mx-auto" /></td>
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-6 bg-slate-700/50 rounded animate-pulse mx-auto" /></td>
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-6 bg-slate-700/50 rounded animate-pulse mx-auto" /></td>
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-6 bg-slate-700/50 rounded animate-pulse mx-auto" /></td>
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-6 bg-slate-700/50 rounded animate-pulse mx-auto" /></td>
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-6 bg-slate-700/50 rounded animate-pulse mx-auto" /></td>
      <td className="py-2 px-1.5 md:px-2"><div className="h-4 w-6 bg-slate-700/50 rounded animate-pulse mx-auto" /></td>
    </tr>
  );
}

export function MatchdayListSkeleton() {
  return (
    <div className="space-y-3">
      {[1, 2, 3].map((i) => (
        <div key={i} className="flex items-center justify-between p-4 rounded-xl bg-slate-900/40 border border-primary/10 animate-pulse">
          <div className="space-y-2">
            <div className="h-4 w-28 bg-slate-700/50 rounded" />
            <div className="h-3 w-20 bg-slate-700/30 rounded" />
          </div>
          <div className="h-9 w-16 bg-slate-700/50 rounded-lg" />
        </div>
      ))}
    </div>
  );
}

export function TopFiveSkeleton() {
  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-3 md:gap-6">
      {[1, 2, 3, 4, 5].map((i) => (
        <div key={i} className="flex flex-col items-center animate-pulse">
          <div className="w-14 h-14 md:w-20 md:h-20 rounded-lg bg-slate-700/50" />
          <div className="h-4 w-16 bg-slate-700/50 rounded mt-2" />
          <div className="h-5 w-10 bg-slate-700/50 rounded mt-1" />
        </div>
      ))}
    </div>
  );
}
