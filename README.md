# Funnelius

<kbd><img src="images/Screenshot.png" alt="My Image with Gray Border" style="border:2px solid #000;"></kbd>

**Funnelius** is an open-source Python library designed to analyze and visualize complex, non-linear user funnels. Built with Pandas, Graphviz, and Streamlit, it allows data scientists and analysts to easily track user journeys, calculate conversion rates, and identify bottlenecks.

## Prerequisites
Please ensure that you have **graphviz** and **streamlit** installed and these tools can be accessed in your Operation System's path.

## Features

- **Visualize Complex Funnels**: Analyze and visualize non-linear, conditional funnels where the next step depends on previous answers or actions.
- **Conversion Rate Calculation**: Automatically calculates conversion rates at each step of the funnel.
- **Bottleneck Detection**: Uses conditional formatting to highlight steps with low conversion rates or long durations.
- **Comparison**: Ability to compare two funnel data and see differences.
- **Answe Contribution**: Show answer contribution of every step and changes.
- **Filter and Clean Data**: Filters out noise and irrelevant data to focus on the most important routes.
- **Interactive UI**: Powered by Streamlit, providing a GUI to tweak parameters and see changes instantly.
- **PDF Export**: Generate funnel visualizations as PDF files.

## Installation

To install **Funnelius**, use pip:

```bash
pip install funnelius
```

## Usage

Here’s a quick example of how to use Funnelius to analyze a funnel:

1- Import the library:

```python
import funnelius as fa
```
2- Prepare your funnel data in a pandas DataFrame with these structure:

| user_id | action | action_start | answer |
|----------|----------|----------|----------|
| 1 | 1st question | 2025-04-10 12:04:15.00 | Yes | 
| 1 | 2nd question | 2025-04-10 12:05:17.00 | No |

Render the funnel analysis:

```python
fa.render(df)
```

Funnelius will process this data and generate a visual funnel with conversion rates, drop-off percentages, and more.

You can pass this optional parameters to fine tune funnel: 

- **df:** The input pandas DataFrame containing user journey data with user_id, action, and action_start columns.

- **title:** Filename (without extension) used for exporting the final funnel visualization as a PDF.

- **first_actions_filter:** Optional list of starting actions to include; filters out journeys that begin with other actions.

- **goals:** List of actions that define successful completion of the journey (used to calculate conversion).

- **max_path_num:** Maximum number of unique user paths to display in the graph; 0 means show all.

- **show_drop:** Boolean flag to include or exclude drop-off data from the funnel visualization.

- **show_answer:** Boolean flag to shw/hide answer  contribution in the funnel visualization.

- **comparison_df:** The DataFrame containing user journey data that you want to use to compare.

- **gradient:** A list with length 3 that contains gradient color data in RGB points. for example: gradient = [[255,205,205],[255,255,255],[205,255,205]] 

- **gradient_metric:** Metric that should be used for condtional formatting. Possible values are: **users**, **conversion-rate**, **percent-of-total** and **duration-median**

- **metrics:** A list of metrics to show in every step. Possible values are: **users**, **conversion-rate**, **percent-of-total** and **duration-median**

### Streamlit GUI

The library includes an interactive user interface powered by Streamlit, allowing you to visualize and tweak funnel parameters.

Run the app python:

```python
import funnelius as f
f.interactive()
```
screenshot:

<kbd><img src="images/Screenshot.png" alt="My Image with Gray Border" style="border:2px solid #000;"></kbd>

## Contributing

I welcome contributions! Feel free to open issues, or submit pull requests to help improve Funnelius.

## License

Funnelius is open-source software licensed under the Apache 2.0 License.


## Contact

For questions or feedback, please reach out via GitHub Issues.

Happy funnel analyzing! 🚀
