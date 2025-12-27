# LOG8415E Final Assignment - Cloud Design Patterns

## Quick Start

```powershell
# 1. Clone and setup
cd C:\Users\abdel\Desktop\log8415e-final-assignment
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt

# 2. Configure AWS credentials
aws configure
# Enter: Access Key ID, Secret Access Key, Region: us-east-1

# 3. Deploy everything
python deploy.py

# 4. Cleanup when done
python deploy.py --destroy
```

---

## Architecture

```
Internet
    │
    ▼
┌─────────────┐
│ GATEKEEPER  │  ← Public (port 8080)
│  t3.small   │    Auth + Validation
└─────────────┘
    │ (private)
    ▼
┌─────────────┐
│   PROXY     │  ← Private only
│  t3.small   │    Routing strategies
└─────────────┘
    │ (private)
    ▼
┌─────────────────────────────────┐
│         MySQL Cluster           │
├───────────┬───────────┬─────────┤
│  MANAGER  │  WORKER1  │ WORKER2 │
│ t3.micro  │ t3.micro  │ t3.micro│
│  (master) │ (replica) │(replica)│
└───────────┴───────────┴─────────┘
```

---

## Commands

### Full Automation (deploy.py)

```powershell
python deploy.py                 # Full deployment (all phases)
python deploy.py --phase 1       # Phase 1 only (DB cluster)
python deploy.py --phase 2       # Phase 2 only (Proxy + Gatekeeper)
python deploy.py --phase 3       # Phase 3 only (Benchmarks)
python deploy.py --status        # Show current status
python deploy.py --destroy       # Destroy all resources
```

### Individual Commands (main.py)

```powershell
python main.py deploy-db         # Create DB instances
python main.py setup-db          # Install MySQL via SSH
python main.py deploy-db-full    # Both in one command
python main.py status-db         # Show DB status
python main.py verify-db         # Verify MySQL/Sakila
python main.py destroy-db        # Destroy DB instances
python main.py ssh manager       # Get SSH command
```

---

## Project Structure

```
log8415e-final-assignment/
├── deploy.py              # Main automation script
├── main.py                # CLI commands
├── benchmark.py           # Benchmark runner (Phase 3)
├── requirements.txt       # Python dependencies
├── config/
│   └── settings.yaml      # Configuration (AWS, MySQL, etc.)
├── infrastructure/        # AWS boto3 code
│   ├── aws_client.py      # AWS session management
│   ├── config.py          # Config loader
│   ├── db_nodes.py        # EC2 instance management
│   ├── keypair.py         # Key pair management
│   ├── security_groups.py # Security groups
│   ├── ssh_client.py      # SSH via paramiko
│   └── setup_db.py        # MySQL setup (Python)
├── application/           # Proxy & Gatekeeper (Phase 2)
│   ├── proxy.py
│   ├── gatekeeper.py
│   └── strategies.py
├── results/               # Benchmark results
└── report/                # LaTeX report
```

---

## Phases

### Phase 1: Database Cluster ✅
- 3x t3.micro EC2 instances
- MySQL 8 installed via SSH
- Sakila database imported
- Sysbench benchmark executed

### Phase 2: Proxy + Gatekeeper (TODO)
- MySQL replication (Manager → Workers)
- Proxy with 3 routing strategies
- Gatekeeper with auth + validation
- Strict security groups

### Phase 3: Benchmarks (TODO)
- 1000 writes + 1000 reads per strategy
- Strategies: direct_hit, random, customized
- Results saved to JSON

---

## Configuration

Edit `config/settings.yaml`:

```yaml
aws:
  region: us-east-1

mysql:
  root_password: "SecureRoot123!"
  app_user: appuser
  app_password: "AppUser456!"

gatekeeper:
  api_key: "your-secret-key"
```

---

## Estimated Costs

| Resource | Type | Count | Cost/hour |
|----------|------|-------|-----------|
| DB Nodes | t3.micro | 3 | ~$0.03 |
| Proxy | t3.small | 1 | ~$0.02 |
| Gatekeeper | t3.small | 1 | ~$0.02 |
| **Total** | | **5** | **~$0.07** |

**Remember to run `python deploy.py --destroy` when done!**
