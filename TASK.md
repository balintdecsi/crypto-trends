Get data from a public source: Use a publicly available API or an easily scrapable website. Collect the data into a SQL or noSQL database that better fits your chosen data structure. The data could be stock market prices, marketplace ads, local restaurant lunch menus – choose a source that is relevant to your business. Tip: please do not use SQLite, as it makes it difficult to evaluate your solution. Thank you.

Process the data: Perform some useful operation on the data that can have business value. You do not need to build a machine learning model; we are more interested in how you analyze the raw data and what information you can extract from it. For example, you could examine price changes, trends, or demand fluctuations for a given product category.

Publish the solution: Make your solution available locally as a frontend application or REST API, where the results are easily accessible. The frontend can be a simple dashboard or interactive web interface, while the API should provide the ability to query the processed data and return the results.

Technical requirements

Programming language: Solve the task in a programming language that you are comfortable working in. Tip: Python is an advantage for us.

Architecture: The solution should be based on a microservice architecture that modularizes the data collection, processing, and publishing steps.

Testability: It is important that your completed code can be tested locally. Please ensure that all necessary descriptions and configurations are available on GitHub. Tip: use Docker so that we can easily test your solution. If you don't have time to deploy while solving the task, remember in the Readme how you would do it.

Documentation. We ask that you document your work at a basic level. In a few words, describe why you chose to solve the given problem, why you worked with the given technologies, and how the main parts of your solution work.