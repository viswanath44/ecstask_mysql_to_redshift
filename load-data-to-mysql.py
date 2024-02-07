import pandas as pd
import numpy as np

num_rows = 700000

data = {
    "id": np.arange(1, num_rows + 1),
    "name": ["Name_" + str(i) for i in range(1, num_rows + 1)],
    "age": np.random.randint(18, 65, size=num_rows),
    # Add more columns as needed
}

df = pd.DataFrame(data)
