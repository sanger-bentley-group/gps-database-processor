#!/usr/bin/env python

import pandas as pd
import argparse
import sys
import os


def main():
    args = parse_arguments()
    
    df_results, df_info, table2_path, table3_path, df_table2, df_table3 = args_check(args)
    
    df_table2_new_data = generate_table2_data(df_results, df_info, args.assembler)
    df_table3_new_data = generate_table3_data(df_results, df_info)

    integrate_table2(df_table2_new_data, df_table2, table2_path)


# Check files/paths actually exist, and load them into dataframes and save paths
def args_check(args):
    try:
        df_results = pd.read_csv(args.results, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        sys.exit(f"Error: {args.results} is not found!")
    
    try:
        df_info = pd.read_csv(args.info, dtype=str, keep_default_na=False)
        
        if (missing_laneids := (set(df_results["Sample_ID"]) - set(df_info["Lane_id"]))):
            sys.exit(f"Error: Information of the following Lane ID(s) are not provided: {', '.join(missing_laneids)}")

        optional_columns = ["Supplier_name", "Sanger_sample_id", "ERR", "ERS"]
        for col in optional_columns:
            if col not in df_info.columns:
                df_info[col] = "_"
            else:
                df_info[col].replace("", "_", inplace=True)
    except FileNotFoundError:
        sys.exit(f"Error: {args.info} is not found!")


    if not os.path.isdir(args.data):
        sys.exit(f"Error: {args.data} is not a valid directory path!")
    try:
        table2_path = os.path.join(args.data, "table2.csv")
        table3_path = os.path.join(args.data, "table3.csv")

        df_table2 = pd.read_csv(table2_path, dtype=str, keep_default_na=False)
        df_table3 = pd.read_csv(table3_path, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        sys.exit(f"Error: table2.csv and/or table3.csv are not found in {args.data}!")

    return df_results, df_info, table2_path, table3_path, df_table2, df_table3


# Generate table2 data for integration
def generate_table2_data(df_results, df_info, assembler):
    df_table2_new_data = df_results.copy()

    df_table2_new_data = df_table2_new_data.merge(df_info, left_on="Sample_ID", right_on="Lane_id", how="left")

    # Add used assembler based on user input
    df_table2_new_data["Assembler"] = assembler
    # Add legacy column
    df_table2_new_data["Proportion_of_Het_SNPs"] = "_"

    # Rename columns that are not in table2 format
    df_table2_new_data.rename(
        columns = {
            "S.Pneumo_%":  "Streptococcus_pneumoniae",
            "Assembly_Length":  "Total_length",
            "Contigs#":  "No_of_contigs",
            "Ref_Cov_%":  "Genome_covered",
            "Seq_Depth":  "Depth_of_coverage",
            "Overall_QC":  "QC",
            "Het-SNP#": "Hetsites_50bp"
        },
        inplace=True
    )

    # Extract and reorder relevant columns
    df_table2_new_data = df_table2_new_data[["Lane_id", "Public_name" ,"Assembler", "Streptococcus_pneumoniae", "Total_length", "No_of_contigs", "Genome_covered", "Depth_of_coverage", "Proportion_of_Het_SNPs", "QC", "Supplier_name", "Hetsites_50bp"]]

    return df_table2_new_data


def generate_table3_data(df_results, df_info):
    df_table3_new_data = df_results[df_results["Overall_QC"] == "PASS"].copy()
    pass


def integrate_table2(df_table2_new_data, df_table2, table2_path):
    # Ensure new Lane_id(s) do not exist in the existing table2
    if already_exist_lane_id := set(df_table2["Lane_id"]).intersection(df_table2_new_data["Lane_id"]):
        sys.exit(f"Error: The following Lane_ID(s) already exist in {table2_path}: {', '.join(sorted(already_exist_lane_id))}.")

    pd.concat([df_table2, df_table2_new_data], axis=0).to_csv(table2_path, index=False)


def parse_arguments():
    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description='Integrate GPS Pipeline results into gps2-data compatible table2.csv and table3.csv',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '-r', '--results',
        default='results.csv',
        help='path to results.csv generated by the GPS Pipeline'
    )

    parser.add_argument(
        '-i', '--info',
        default='info.csv',
        help='path to info.csv which contain at least 2 comma-separated columns (first two are required, others are optional): Lane_id, Public_name, Supplier_name, Sanger_sample_id, ERR, ERS'
    )

    parser.add_argument(
        '-d', '--data',
        default=os.getcwd(),
        help='path to gps2-data compatible directory that is holding table2.csv and table3.csv'
    )

    parser.add_argument(
        '-a', '--assembler',
        default='SHOVILL',
        help='de novo assembler used in the run'
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()