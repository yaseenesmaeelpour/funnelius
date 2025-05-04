import funnelius as f
import pandas as pd
df = pd.read_csv('../sample_data_vehicle.csv')
f.render(df)
f.interactive()
