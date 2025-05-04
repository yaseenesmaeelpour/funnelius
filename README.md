# Funnelius

**Funnelius** is an open-source Python library designed to analyze and visualize complex, non-linear user funnels. Built with Pandas, Graphviz, and Streamlit, it allows data scientists and analysts to easily track user journeys, calculate conversion rates, and identify bottlenecks in real-time.

## Features

- **Visualize Complex Funnels**: Analyze non-linear, conditional funnels where the next step depends on previous answers or actions.
- **Conversion Rate Calculation**: Automatically calculates conversion rates at each step of the funnel.
- **Bottleneck Detection**: Uses conditional formatting to highlight steps with low conversion rates or long durations.
- **Filter and Clean Data**: Filters out noise and irrelevant data to focus on the most important routes.
- **Interactive UI**: Powered by Streamlit, providing a GUI to tweak parameters and see changes instantly.
- **PDF Export**: Generate and download funnel visualizations as PDF files.

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

| user_id | action | action_start |
|----------|----------|----------|
| 1 | 1st question | 2025-04-10 12:04:15.00 | 
| 1 | 2nd question | 2025-04-10 12:05:17.00 | 

Render the funnel analysis:

```python
fa.render(df)
```


Funnelius will process this data and generate a visual funnel with conversion rates, drop-off percentages, and more.

### Streamlit GUI

The library includes an interactive user interface powered by Streamlit, allowing you to visualize and tweak funnel parameters in real-time.

Run the app with:
```bash
streamlit run interactive.py
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
