# Dashboard Files - GitHub Push Instructions

## ✅ What's Been Created

All Maven Fuzzy Factory dashboard files are ready at:
`/Workspace/Users/keyegon@gmail.com/ecommerce-intelligence-platform/dashboards/maven-fuzzy-factory/`

### File Summary:
- **README.md** - Comprehensive dashboard documentation (11.9 KB)
- **dashboard-config.json** - Dashboard metadata (4.4 KB)
- **33 SQL query files** organized by page:
  * `queries/executive/` - 6 queries
  * `queries/funnel/` - 9 queries
  * `queries/products/` - 9 queries
  * `queries/customer_ml/` - 9 queries
- **Documentation:**
  * `docs/filter-guide.md` - Filter usage guide (8.0 KB)
  * `docs/data-sources.md` - Data dictionary (14.2 KB)

**Total: 37 files, ~45 KB**

---

## 🚀 Option 1: Push Using Databricks Repos (RECOMMENDED)

Databricks Repos provides native Git integration in the workspace.

### Steps:

1. **Go to Databricks Repos**
   - In workspace sidebar, click "Repos"
   - Click "Add Repo"

2. **Connect to GitHub Repository**
   - Git repository URL: `https://github.com/erickyegon/ecommerce-intelligence-platform`
   - Click "Create Repo"
   - Authenticate with your GitHub credentials/token

3. **Copy Dashboard Files to Repo**
   ```bash
   # In a Databricks notebook or terminal:
   cp -r /Workspace/Users/keyegon@gmail.com/ecommerce-intelligence-platform/dashboards \
         /Repos/keyegon@gmail.com/ecommerce-intelligence-platform/
   ```

4. **Commit and Push from Repos UI**
   - Open the Repo in Databricks
   - Click the Git icon
   - See your changes in "Uncommitted changes"
   - Add commit message: "Add Maven Fuzzy Factory dashboard files"
   - Click "Commit & Push"

---

## 🔧 Option 2: Push Using Command Line

### A. Using the Provided Script

```bash
cd /Workspace/Users/keyegon@gmail.com
./commit-dashboard.sh
```

### B. Manual Git Commands

```bash
cd /Workspace/Users/keyegon@gmail.com/ecommerce-intelligence-platform

# Initialize Git repository
git init

# Configure Git user
git config user.name "Erick Yegon"
git config user.email "keyegon@gmail.com"

# Add GitHub remote
git remote add origin https://github.com/erickyegon/ecommerce-intelligence-platform.git

# Stage dashboard files
git add dashboards/

# Commit with message
git commit -m "Add Maven Fuzzy Factory dashboard SQL queries and documentation"

# Push to GitHub (you'll be prompted for credentials)
git push -u origin main
```

### C. Authentication

If the repository is private, you'll need to authenticate:

**Using Personal Access Token (PAT):**
```bash
# When prompted for password, use your GitHub Personal Access Token
# Create PAT at: https://github.com/settings/tokens

# Or configure credential helper:
git config --global credential.helper store
```

**Using SSH:**
```bash
# Use SSH remote URL instead:
git remote set-url origin git@github.com:erickyegon/ecommerce-intelligence-platform.git
```

---

## 📂 Repository Structure After Push

```
ecommerce-intelligence-platform/
├── README.md (existing)
├── ARCHITECTURE.md (existing)
├── notebooks/ (existing)
├── sql/ (existing)
├── dashboards/                           ← NEW
│   └── maven-fuzzy-factory/
│       ├── README.md                     ← Dashboard docs
│       ├── dashboard-config.json          ← Metadata
│       ├── queries/
│       │   ├── executive/                ← 6 SQL files
│       │   ├── funnel/                   ← 9 SQL files
│       │   ├── products/                 ← 9 SQL files
│       │   └── customer_ml/              ← 9 SQL files
│       └── docs/
│           ├── filter-guide.md           ← Filter documentation
│           └── data-sources.md           ← Data dictionary
```

---

## 🔍 Verify Files Before Pushing

```bash
cd /Workspace/Users/keyegon@gmail.com/ecommerce-intelligence-platform

# List all dashboard files
find dashboards/ -type f

# Count files
find dashboards/ -type f | wc -l

# Check total size
du -sh dashboards/
```

---

## ✅ Success Checklist

After pushing, verify on GitHub:

- [ ] Navigate to https://github.com/erickyegon/ecommerce-intelligence-platform
- [ ] Confirm `dashboards/maven-fuzzy-factory/` directory exists
- [ ] Check README.md renders correctly
- [ ] Verify all SQL query files are present (33 files)
- [ ] Ensure documentation files are readable
- [ ] Review dashboard-config.json structure

---

## 🆘 Troubleshooting

### "fatal: not a git repository"
```bash
# Initialize Git first:
cd /Workspace/Users/keyegon@gmail.com/ecommerce-intelligence-platform
git init
git remote add origin https://github.com/erickyegon/ecommerce-intelligence-platform.git
```

### "remote: Repository not found"
- Verify repository exists at GitHub URL
- Check you have access permissions
- Ensure correct repository name

### Authentication Issues
- Use Personal Access Token (PAT) instead of password
- Create PAT with `repo` scope at https://github.com/settings/tokens
- For HTTPS, enter PAT when prompted for password

### "refusing to merge unrelated histories"
```bash
# If repository already has content:
git pull origin main --allow-unrelated-histories
git push origin main
```

---

## 📖 Additional Resources

- **Databricks Repos Documentation:** https://docs.databricks.com/repos/index.html
- **GitHub PAT Guide:** https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token
- **Dashboard README:** /Workspace/Users/keyegon@gmail.com/ecommerce-intelligence-platform/dashboards/maven-fuzzy-factory/README.md

---

## 📞 Next Steps

1. Choose Option 1 (Databricks Repos) or Option 2 (Command Line)
2. Push dashboard files to GitHub
3. Verify files appear on GitHub repository
4. Share repository URL with team
5. Update main repository README to link to dashboard documentation

**Dashboard ID:** 01f131fa580d10f9a5eef6ab87cf6e70  
**Dashboard URL:** (from Databricks workspace)  
**Repository:** https://github.com/erickyegon/ecommerce-intelligence-platform

---

**Created:** April 6, 2026  
**Status:** ✅ Ready to push
