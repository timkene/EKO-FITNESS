import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Signup from './pages/Signup';
import Login from './pages/Login';
import MemberLayout from './components/MemberLayout';
import Dashboard from './pages/Dashboard';
import Matchday from './pages/Matchday';
import Leaderboard from './pages/Leaderboard';
import Rules from './pages/Rules';
import Profile from './pages/Profile';
import Admin from './pages/Admin';
import { ToastProvider } from './components/Toast';
import './App.css';

function App() {
  return (
    <ToastProvider>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Navigate to="/login" replace />} />
        <Route path="/signup" element={<Signup />} />
        <Route path="/login" element={<Login />} />
        <Route element={<MemberLayout />}>
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/matchday" element={<Matchday />} />
          <Route path="/leaderboard" element={<Leaderboard />} />
          <Route path="/rules" element={<Rules />} />
          <Route path="/profile" element={<Profile />} />
        </Route>
        <Route path="/admin" element={<Admin />} />
        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
    </BrowserRouter>
    </ToastProvider>
  );
}

export default App;
