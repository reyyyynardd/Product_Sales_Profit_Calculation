import java.io.File;
import java.util.*;
import java.util.concurrent.*;

//constructor class created for Product and each of their profit
class Product {
    private final String productName;
    private final double productProfit;

    public Product(String productName, double productProfit) {
        this.productName = productName;
        this.productProfit = productProfit;
    }

    public String getProductName() {
        return productName;
    }

    public double getProductProfit() {
        return productProfit;
    }
}

//constructor class for Branch and amount of products sold at each branch
class Branch {
    private final String branchId;
    private final Map<Product, Integer> productsSold;

    public Branch(String branchId) {
        this.branchId = branchId;
        this.productsSold = new HashMap<>();
    }

    public String getBranchId() {
        return branchId;
    }

    public Map<Product, Integer> getProductsSold() {
        return productsSold;
    }
}

//counter class created to be used as variable for value summation
class Counter {
    private double value = 0;

    public synchronized double getValue() {
        return value;
    }

    public synchronized void increaseValue(double increment) {
        value += increment;
    }
}

//iteration counter initialized to track number of iterations and if the maximum iterations of data has been reached
class MaxIterationCounter {
    private final int maxIterations;
    private int currentIterations = 0;

    public MaxIterationCounter(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    public synchronized void incrementIterations() {
        if (currentIterations < maxIterations) {
            currentIterations++;
        }
    }

    //boolean used to check if maximum iterations has been reached
    public synchronized boolean isMaxed() {
        return currentIterations == maxIterations;
    }
}

//Comparer class created to compare and find minimum value
class Comparer<T> {
    private T item;
    private double value;
    //use boolean to check if any comparisons have been made
    private boolean isFirstComparison = true;

    public synchronized T getItem() {
        return item;
    }

    public synchronized double getValue() {
        return value;
    }

    //comparer to identify minimum value
    public synchronized void compareItems(T item, double value) {
        if (isFirstComparison || value < this.value) {
            this.item = item;
            this.value = value;
            isFirstComparison = false;
        }
    }
}

//class created to display final output
class Report {
    //array initialized for each product
    private final Product[] products;
    //task (i) to calculate totalUnitsSold
    private final Map<Product, Counter> totalUnitsSold = new HashMap<>();
    //task (ii) to calculate totalDailyProfits across all branches
    private final Counter totalDailyProfits = new Counter();
    //task (iii) to identify the branch with the lowest profit
    private final Comparer<Branch> lowestProfitBranch = new Comparer<>();
    //initialize tracker using iteration counter
    private final MaxIterationCounter progressTracker;

    //constructor initialized for the report
    public Report(Product[] products) {
        this.products = products;
        this.progressTracker = new MaxIterationCounter(products.length + 1);
        for (Product product : products) {
            totalUnitsSold.put(product, new Counter());
        }
    }

    public Map<Product, Counter> getTotalUnitsSold() {
        return totalUnitsSold;
    }

    public Counter getTotalDailyProfits() {
        return totalDailyProfits;
    }

    public Comparer<Branch> getLowestProfitBranch() {
        return lowestProfitBranch;
    }

    public MaxIterationCounter getProgressTracker() {
        return progressTracker;
    }

    //Print output
    public void printReport() {
        //Print table header
        System.out.println("+-----------------+-----------------+");
        System.out.println("| Product Name    | Total Units Sold|");
        System.out.println("+-----------------+-----------------+");

        //Print total units sold for each product
        for (Product product : products) {
            String productName = product.getProductName();
            double totalUnits = totalUnitsSold.get(product).getValue();

            System.out.printf("| %-15s | %15.0f |%n", productName, totalUnits);
        }

        //Print total daily profits
        System.out.println("+-----------------+-----------------+");
        System.out.printf("| Total Daily Profits: $%12.2f|%n", totalDailyProfits.getValue());
        System.out.println("+-----------------+-----------------+");

        //Print branch with the lowest profit
        System.out.println("| Branch with the lowest profit:    |");
        System.out.printf("| %-30s    |%n", lowestProfitBranch.getItem().getBranchId());
        System.out.println("+-----------------+-----------------+");
    }
}

//task initialized to calculate number of products sold
class BranchProductsSold implements Runnable {
    private final Branch branch;
    private final Product product;
    private final Report report;
    private final MaxIterationCounter branchIterations;

    //constructor for products sold
    public BranchProductsSold(Branch branch, Product product, Report report, MaxIterationCounter branchIterations) {
        this.branch = branch;
        this.product = product;
        this.report = report;
        this.branchIterations = branchIterations;
    }

    @Override
    //run method to calculate the number of products sold
    public void run() {
        double increment = branch.getProductsSold().get(product);

        //synchronize report data once iterations are maxed
        synchronized (report) {
            report.getTotalUnitsSold().get(product).increaseValue(increment);
            branchIterations.incrementIterations();
            if (branchIterations.isMaxed()) {
                report.getProgressTracker().incrementIterations();
                if (report.getProgressTracker().isMaxed()) {
                    report.printReport();
                }
            }
        }
    }
}

//task initialized to calculate profits
class BranchProductsProfits implements Runnable {
    private final Branch branch;
    private final Product product;
    private final Report report;
    private final MaxIterationCounter overallIterations;
    private final Counter branchProfits;
    private final MaxIterationCounter productIterations;

    //constructor for product profits
    public BranchProductsProfits(Branch branch, Product product, Report report, MaxIterationCounter overallIterations, Counter branchProfits, MaxIterationCounter productIterations) {
        this.branch = branch;
        this.product = product;
        this.report = report;
        this.overallIterations = overallIterations;
        this.branchProfits = branchProfits;
        this.productIterations = productIterations;
    }

    @Override
    //run method used to calculate the total daily profit and lowest profit
    public void run() {
        double increment = branch.getProductsSold().get(product) * product.getProductProfit();

        //synchronize report data once iterations are maxed
        synchronized (report) {
            branchProfits.increaseValue(increment);
            overallIterations.incrementIterations();
            productIterations.incrementIterations();
            if (productIterations.isMaxed()) {
                report.getTotalDailyProfits().increaseValue(branchProfits.getValue());
                report.getLowestProfitBranch().compareItems(branch, branchProfits.getValue());
            }
            if (overallIterations.isMaxed()) {
                report.getProgressTracker().incrementIterations();
                if (report.getProgressTracker().isMaxed()) {
                    report.printReport();
                }
            }
        }
    }
}

//Driver class to run the program
public class Main {
    public static void main(String[] args) {
        //initialize all products within an array
        Product[] products = {
                new Product("Product A", 1.10),
                new Product("Product B", 1.50),
                new Product("Product C", 2.10),
                new Product("Product D", 1.60),
                new Product("Product E", 1.80),
                new Product("Product F", 3.90)
        };

        //initialize array list to store all branches
        List<Branch> branches = new ArrayList<>();

        //read sales_records.csv data to use for calculation
        try {
            File file = new File("sales_records.csv");
            Scanner scanner = new Scanner(file);
            scanner.nextLine(); //skip header

            while (scanner.hasNextLine()) {
                List<String> lineData = Arrays.asList(scanner.nextLine().split(","));
                Branch branch = new Branch(lineData.get(0));

                for (int i = 1; i < lineData.size(); i++) {
                    branch.getProductsSold().put(products[i - 1], Integer.parseInt(lineData.get(i)));
                }
                branches.add(branch);
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //initialize report to display output
        Report report = new Report(products);

        //initialize map for number of branches
        Map<Product, MaxIterationCounter> numberOfBranches = new HashMap<>();
        for (Product product : products) {
            numberOfBranches.put(product, new MaxIterationCounter(branches.size()));
        }

        //initialize total number of data entries
        MaxIterationCounter numberOfDataEntries = new MaxIterationCounter(products.length * branches.size());

        //initialize executor threads
        ExecutorService executor = Executors.newFixedThreadPool(2);

        for (Branch branch : branches) {
            Counter profitsByBranch = new Counter();
            MaxIterationCounter noOfProducts = new MaxIterationCounter(products.length);

            //submit runnable task to executor
            for (Product product : products) {
                executor.submit(new BranchProductsSold(branch, product, report, numberOfBranches.get(product)));
                executor.submit(new BranchProductsProfits(branch, product, report, numberOfDataEntries, profitsByBranch, noOfProducts));
            }
        }
        //shutdown executor
        executor.shutdown();
    }
}
