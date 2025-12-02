# Admin Panel - README

## ✅ Status: **COMPLETE & READY FOR DEPLOYMENT**

Complete admin panel implementation for Operabot with role-based access control, user management, and Telegram interface.

## 🚀 Quick Start

### 1. Apply Database Migrations

```bash
cd /Users/vitalyefimov/Projects/operabot
mysql -u your_user -p your_database < migrations/run_migrations.sql
```

### 2. Configure Environment

Set bootstrap admin in `.env`:

```bash
SUPREME_ADMIN_ID=123456789  # Your Telegram user ID
# OR
SUPREME_ADMIN_USERNAME=your_username
```

### 3. Start Bot

```bash
python app/main.py
```

### 4. Access Admin Panel

As the supreme admin user:
```
/admin
```

## 📋 Features Implemented

### ✅ Role System
- **Operator**: Basic access
- **Admin**: Can approve users, promote to admin
- **Superadmin**: Full access, can create admins
- **Supreme/Dev Admin**: Bootstrap admins from config

### ✅ User Management
- View pending requests (`/admin` → 👥 Операторы)
- Approve/decline users
- Block/unblock users
- Promote/demote roles

### ✅ Commands
- `/admin` - Admin panel menu
- `/approve <user_id>` - Quick approve
- `/make_admin <user_id>` - Promote to admin
- `/make_superadmin <user_id>` - Promote to superadmin (supreme only)
- `/admins` - List all admins

### ✅ Statistics
- Live dashboard with user counts
- Weekly quality metrics
- Admin action audit log

### ✅ Notifications
- New user requests → All admins
- Approval/decline → User
- Promotion → User
- Admin actions → Admins (optional)

## 📊 Architecture

```
app/
├── db/
│   ├── models.py (UserRecord, AdminActionLog)
│   └── repositories/
│       └── admin.py (AdminRepository)
├── services/
│   └── notifications.py (NotificationsManager)
├── telegram/
│   ├── middlewares/
│   │   └── permissions.py (PermissionsManager)
│   └── handlers/
│       ├── admin_panel.py (Main menu)
│       ├── admin_users.py (User management)
│       ├── admin_commands.py (Quick commands)
│       └── admin_stats.py (Statistics)
└── main.py (Integration)

migrations/
├── 001_admin_roles.sql
├── 002_admin_audit.sql
└── 003_call_lookup_fields.sql

tests/
└── unit/
    └── test_admin_panel.py
```

## 🗄️ Database Schema

### users table (modified)
- `role` ENUM: operator/admin/superadmin
- `status` ENUM: pending/approved/blocked
- `approved_by` INT
- `blocked_at` TIMESTAMP
- `operator_id` INT

### admin_action_logs table (new)
- Tracks all admin actions
- JSON payload for details
- Links to actor and target users

## 🧪 Testing

```bash
# Run admin panel tests
pytest tests/unit/test_admin_panel.py -v

# Run all tests
pytest tests/ -v --cov=app
```

## 📖 Usage Examples

### Bootstrap First Admin

1. Set `SUPREME_ADMIN_ID` in .env
2. Start bot
3. Send `/admin` from that user ID
4. You now have admin access

### Approve New User

When a user sends `/start`, admins receive notification:

```
🔔 Новая заявка на доступ

👤 Пользователь: Ivan Ivanov
📱 Username: @ivan
📞 Extension: 101

/approve 42 - Утвердить
```

Approve via:
- `/approve 42`
- OR: `/admin` → 👥 Операторы → Select user → ✅ Approve

### Promote User

```bash
/make_admin 42  # Promote to admin
/make_superadmin 42  # Promote to superadmin (supreme only)
```

### View System Stats

```
/admin → 📈 Статистика
```

Shows:
- Pending user count
- Admin count
- Weekly call metrics
- Quality scores

## 🔐 Security

- Role hierarchy enforced
- Supreme admin from config only
- All actions logged to `admin_action_logs`
- Permissions checked on every operation
- Status='approved' required for access

## 📝 Implementation Status

✅ **100% Core Features Complete**

| Phase | Status | Description |
|-------|--------|-------------|
| 1. Database | ✅ | Migrations, models |
| 2. Config | ✅ | Env vars, permissions |
| 3. Repository | ✅ | AdminRepository CRUD |
| 4. Handlers | ✅ | Telegram UI |
| 5. Notifications | ✅ | Alerts |
| 6. Call Lookup* | 🔜 | LM metrics (future) |
| 7. Stats | ✅ | Dashboard |
| 8. Integration | ✅ | main.py |
| 9. Tests | ✅ | Unit tests |

*Call lookup LM extensions planned for future release

## 🐛 Troubleshooting

### "User not found in DB"
- User needs to send `/start` first
- System creates user record on first interaction

### "Недостаточно прав"
- Check user has role='admin' or higher
- Check user status='approved'
- Verify SUPREME_ADMIN_ID matches your user ID

### "Migrations failed"
- Check MySQL user has ALTER TABLE privileges
- Verify database name in connection string
- Review migration logs for specific errors

## 📚 Documentation

- [Implementation Plan](file:///Users/vitalyefimov/.gemini/antigravity/brain/094e0be7-f705-4984-8890-0f14f698a287/implementation_plan.md)
- [Task Checklist](file:///Users/vitalyefimov/.gemini/antigravity/brain/094e0be7-f705-4984-8890-0f14f698a287/task.md)
- [Walkthrough](file:///Users/vitalyefimov/.gemini/antigravity/brain/094e0be7-f705-4984-8890-0f14f698a287/walkthrough.md)
- [Error Handling](file:///Users/vitalyefimov/Projects/operabot/docs/ERROR_HANDLING.md)

## 🎯 Next Steps (Optional Enhancements)

- [ ] Admin management UI (currently via commands)
- [ ] LM metrics in call lookup
- [ ] Advanced filtering in user lists
- [ ] Bulk actions (approve multiple)
- [ ] Export admin action logs
- [ ] Scheduled reports for admins

---

**Project Status**: ✅ Production Ready

All core admin panel functionality is implemented, tested, and integrated into the main application.
