from src.data_loader import load_data
from src.analysis import demand_forecasting, abc_analysis, eoq_analysis, reorder_point_analysis, lead_time_analysis, vendor_analysis, sales_trend_analysis

if __name__ == "__main__":
    data = load_data()

    print("\n📈 Demand Forecasting:")
    forecast_df = demand_forecasting.forecast_demand(data["sales"])
    forecast_df.show(5)

    print("\n🔠 ABC Classification:")
    abc_df = abc_analysis.abc_classification(data["sales"])
    abc_df.show(5)

    print("\n📦 EOQ Analysis:")
    eoq_df = eoq_analysis.calculate_eoq(data["purchase_final"])
    eoq_df.show(5)

    print("\n🔁 Reorder Point Analysis:")
    reorder_df = reorder_point_analysis.reorder_point(data["purchase_final"])
    reorder_df.show(5)

    print("\n⏱️ Lead Time Analysis:")
    lead_time_df = lead_time_analysis.analyze_lead_time(data["purchase_final"])
    lead_time_df.show(5)

    print("\n📋 Vendor Efficiency:")
    vendor_df = vendor_analysis.analyze_vendor_efficiency(data["invoice_purchases"])
    vendor_df.show(5)

    print("\n📊 Sales Trend Analysis:")
    trend_df = sales_trend_analysis.analyze_sales_trend(data["sales"])
    trend_df.show(5)
