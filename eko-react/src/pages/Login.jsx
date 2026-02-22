import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { login } from '../api';
import './Auth.css';

const TOKEN_KEY = 'eko_football_token';
const PLAYER_KEY = 'eko_football_player';

export function setPlayerAuth(token, player) {
  if (token) localStorage.setItem(TOKEN_KEY, token);
  if (player) localStorage.setItem(PLAYER_KEY, JSON.stringify(player));
}

export function getPlayerAuth() {
  const token = localStorage.getItem(TOKEN_KEY);
  const player = JSON.parse(localStorage.getItem(PLAYER_KEY) || 'null');
  return { token, player };
}

export function clearPlayerAuth() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(PLAYER_KEY);
}

export default function Login() {
  const navigate = useNavigate();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    if (!username.trim() || !password) {
      setError('Username and password are required.');
      return;
    }
    setLoading(true);
    try {
      const data = await login(username.trim(), password);
      setPlayerAuth(data.token, data.player);
      navigate('/dashboard', { replace: true });
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Login failed.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="auth-page">
      <div className="auth-card">
        <h1>Log in</h1>
        <p className="sub">Use the username and password from your approval email.</p>

        <form onSubmit={handleSubmit} className="auth-form">
          <label>Username (baller name)</label>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="Baller name"
            autoComplete="username"
          />

          <label>Password</label>
          <div className="password-wrap">
            <input
              type={showPassword ? 'text' : 'password'}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Password"
              autoComplete="current-password"
            />
            <button
              type="button"
              onClick={() => setShowPassword((s) => !s)}
              className="password-toggle"
              aria-label={showPassword ? 'Hide password' : 'Show password'}
              tabIndex={-1}
            >
              <span className="material-symbols-outlined">{showPassword ? 'visibility_off' : 'visibility'}</span>
            </button>
          </div>

          {error && <div className="error-msg">{error}</div>}

          <button type="submit" className="btn primary" disabled={loading}>
            {loading ? 'Logging in...' : 'Log in'}
          </button>
        </form>

        <p className="auth-footer">
          New player? <Link to="/signup">Sign up</Link>
        </p>
        <p className="auth-footer admin-link">
          <Link to="/admin">Admin</Link>
        </p>
      </div>
    </div>
  );
}
