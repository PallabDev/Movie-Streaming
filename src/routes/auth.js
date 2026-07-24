import { Router } from 'express';
import bcrypt from 'bcrypt';
import { db } from '../db/index.js';
import { users } from '../db/schema.js';
import { eq } from 'drizzle-orm';

const router = Router();

router.get('/login', (req, res) => {
  if (req.session?.user) return res.redirect('/');
  res.render('login', { title: 'Login', error: null });
});

router.post('/login', async (req, res) => {
  const { email, password } = req.body;
  try {
    const [user] = await db.select().from(users).where(eq(users.email, email));
    if (!user) {
      return res.render('login', { title: 'Login', error: 'Invalid email or password' });
    }
    const valid = await bcrypt.compare(password, user.password);
    if (!valid) {
      return res.render('login', { title: 'Login', error: 'Invalid email or password' });
    }
    if (!user.hasAccess) {
      return res.render('login', { title: 'Login', error: 'Your account is pending approval. Please contact an admin.' });
    }
    req.session.user = {
      id: user.id,
      name: user.name,
      email: user.email,
      role: user.role,
      hasAccess: user.hasAccess,
      streamsUsed: user.streamsUsed,
      streamLimit: user.streamLimit,
    };
    res.redirect('/');
  } catch (err) {
    console.error('Login error:', err);
    res.render('login', { title: 'Login', error: 'Something went wrong. Try again.' });
  }
});

router.get('/register', (req, res) => {
  if (req.session?.user) return res.redirect('/');
  res.render('register', { title: 'Register', error: null });
});

router.post('/register', async (req, res) => {
  const { name, email, password } = req.body;
  try {
    if (!name || !email || !password) {
      return res.render('register', { title: 'Register', error: 'All fields are required' });
    }
    if (password.length < 6) {
      return res.render('register', { title: 'Register', error: 'Password must be at least 6 characters' });
    }
    const [existing] = await db.select().from(users).where(eq(users.email, email));
    if (existing) {
      return res.render('register', { title: 'Register', error: 'Email already registered' });
    }
    const hashed = await bcrypt.hash(password, 10);
    await db.insert(users).values({ name, email, password: hashed });
    res.render('login', { title: 'Login', error: null, success: 'Account created! Please wait for admin approval.' });
  } catch (err) {
    console.error('Register error:', err);
    res.render('register', { title: 'Register', error: 'Something went wrong. Try again.' });
  }
});

router.get('/logout', (req, res) => {
  req.session.destroy(() => {
    res.redirect('/auth/login');
  });
});

export default router;
