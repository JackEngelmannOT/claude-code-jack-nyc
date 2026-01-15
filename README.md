# NYC Taxi Analytics Challenge

**Time Limit: 2 hours** | **Goal: Close as many issues as possible**

## The Goal

Build a **taxi trip analytics dashboard** using NYC yellow cab data.

Your finished product should:

1. Load and process NYC taxi trip data (parquet format)
2. Display key statistics (total trips, average fare, average distance)
3. Visualize trip patterns (by hour, day, zone)
4. Show busiest pickup/dropoff zones
5. Map visualization with zone boundaries
6. Filter by date, time, borough
7. Be deployed and accessible via a public URL

**How you build it is entirely up to you.** Choose your own stack, architecture, and approach.

## The Data

NYC TLC provides free trip record data in parquet format:

```python
# Quick start (Python)
import pandas as pd
df = pd.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")
```

See [DATA.md](./DATA.md) for detailed field documentation.

## Rules

1. **Fork this repo** - work on your own copy
2. **Close issues by implementing them** - push your code, then close the issue
3. **Be honest** - only close an issue when the feature actually works
4. **Any stack is allowed** - React, Vue, Python, Node, Go... your choice
5. **You may use AI tools** - but you still need to understand and verify your code

## Scoring

Each closed issue = points (see issue labels):

- `1-point` - Quick wins
- `2-points` - Core features
- `3-points` - Advanced features
- `5-points` - Bonus challenges

## Azure Credentials

You have access to Azure for deployment. Credentials will be provided separately.

Good luck!
