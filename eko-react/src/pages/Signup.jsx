import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { signup } from '../api';
import './Auth.css';

export default function Signup() {
  const navigate = useNavigate();
  const [form, setForm] = useState({
    first_name: '',
    surname: '',
    baller_name: '',
    jersey_number: '',
    email: '',
    whatsapp_phone: '',
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState(false);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setForm((prev) => ({ ...prev, [name]: value }));
    setError('');
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    const num = parseInt(form.jersey_number, 10);
    if (!form.first_name?.trim() || !form.surname?.trim() || !form.baller_name?.trim()) {
      setError('First name, surname and baller name are required.');
      return;
    }
    if (!Number.isInteger(num) || num < 1 || num > 100) {
      setError('Jersey number must be between 1 and 100.');
      return;
    }
    if (!form.email?.trim()) {
      setError('Email is required.');
      return;
    }
    if (!form.whatsapp_phone?.trim()) {
      setError('WhatsApp number is required.');
      return;
    }

    setLoading(true);
    try {
      await signup({
        first_name: form.first_name.trim(),
        surname: form.surname.trim(),
        baller_name: form.baller_name.trim(),
        jersey_number: num,
        email: form.email.trim(),
        whatsapp_phone: form.whatsapp_phone.trim(),
      });
      setSuccess(true);
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Sign up failed.');
    } finally {
      setLoading(false);
    }
  };

  if (success) {
    return (
      <div className="auth-page">
        <div className="auth-card success-card">
          <h1>Registration submitted</h1>
          <p>You will receive your login details by email after admin approval.</p>
          <Link to="/login" className="btn primary">Go to Login</Link>
        </div>
      </div>
    );
  }

  return (
    <div className="auth-page">
      <div className="auth-card">
        <h1>Sign up</h1>
        <p className="sub">Join the team — fill the form and wait for approval.</p>

        <form onSubmit={handleSubmit} className="auth-form">
          <label>First name *</label>
          <input
            type="text"
            name="first_name"
            value={form.first_name}
            onChange={handleChange}
            placeholder="First name"
            autoComplete="given-name"
            required
          />

          <label>Surname *</label>
          <input
            type="text"
            name="surname"
            value={form.surname}
            onChange={handleChange}
            placeholder="Surname"
            autoComplete="family-name"
            required
          />

          <label>Baller name *</label>
          <input
            type="text"
            name="baller_name"
            value={form.baller_name}
            onChange={handleChange}
            placeholder="e.g. Flash"
            required
          />

          <label>Jersey number (1–100) *</label>
          <input
            type="number"
            name="jersey_number"
            value={form.jersey_number}
            onChange={handleChange}
            min={1}
            max={100}
            placeholder="e.g. 10"
            required
          />

          <label>Email *</label>
          <input
            type="email"
            name="email"
            value={form.email}
            onChange={handleChange}
            placeholder="your@email.com"
            autoComplete="email"
            required
          />

          <label>WhatsApp phone number *</label>
          <input
            type="tel"
            name="whatsapp_phone"
            value={form.whatsapp_phone}
            onChange={handleChange}
            placeholder="e.g. +234..."
            autoComplete="tel"
            required
          />

          {error && <div className="error-msg">{error}</div>}

          <button type="submit" className="btn primary" disabled={loading}>
            {loading ? 'Submitting...' : 'Sign up'}
          </button>
        </form>

        <p className="auth-footer">
          Already have an account? <Link to="/login">Log in</Link>
        </p>
      </div>
    </div>
  );
}
