#!/usr/bin/env python

import pandas as pd
import argparse
import sys
import os


def main():
    args = parse_arguments()
    
    df_results, df_info, table2_path, table3_path, df_table2, df_table3, df_gpsc_colour, df_serotype_colour = args_check(args)
    
    df_table2_new_data = generate_table2_data(df_results, df_info, args.assembler)
    df_table3_new_data = generate_table3_data(df_results, df_info, df_gpsc_colour, df_serotype_colour)

    # TODO - Re-enable when ready
    # integrate_table2(df_table2_new_data, df_table2, table2_path)
    # integrate_table3(df_table3_new_data, df_table3, table3_path)


# Check files/paths actually exist, and load them into dataframes and save paths
def args_check(args):
    try:
        df_results = pd.read_csv(args.results, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        sys.exit(f"Error: {args.results} is not found!")
    
    try:
        df_info = pd.read_csv(args.info, dtype=str, keep_default_na=False)
        
        if (missing_laneids := (set(df_results["Sample_ID"]) - set(df_info["Lane_id"]))):
            sys.exit(f"Error: Information of the following Lane ID(s) are not provided: {', '.join(sorted(missing_laneids))}")

        optional_columns = ["Supplier_name", "Sanger_sample_id", "ERR", "ERS"]
        for col in optional_columns:
            if col not in df_info.columns:
                df_info[col] = "_"
            else:
                df_info[col] = df_info[col].replace("", "_")
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

    try:
        df_gpsc_colour = pd.read_csv(args.gpsccolour, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        sys.exit(f"Error: {args.gpsccolour} is not found!")
    
    try:
        df_serotype_colour = pd.read_csv(args.serotypecolour, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        sys.exit(f"Error: {args.serotypecolour} is not found!")

    return df_results, df_info, table2_path, table3_path, df_table2, df_table3, df_gpsc_colour, df_serotype_colour


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

    # TODO - Remove test line when done
    df_table2_new_data.to_csv("table2_test.csv", index=False) 

    return df_table2_new_data


def generate_table3_data(df_results, df_info, df_gpsc_colour, df_serotype_colour):
    df_table3_new_data = df_results[df_results["Overall_QC"] == "PASS"].copy()

    df_table3_new_data = df_table3_new_data.merge(df_info, left_on="Sample_ID", right_on="Lane_id", how="left")

    # Convert all content to UPPER case
    for col in df_table3_new_data.columns:
        df_table3_new_data[col] = df_table3_new_data[col].str.upper()

    # Add legacy column
    legacy_columns = ["WGS_SYN", "WGS_SYN_SIR", "WGS_LZO", "WGS_LZO_SIR", "EC"]
    for col in legacy_columns:
        df_table3_new_data[col] = "_"

    # Rename columns that are not in table2 format
    df_table3_new_data.rename(
        columns = {
            "ST": "In_silico_ST",
            "Serotype": "In_silico_serotype",
            "PEN_MIC": "WGS_PEN",
            "PEN_Res(Meningital)": "WGS_PEN_SIR_Meningitis",
            "PEN_Res(Non-meningital)": "WGS_PEN_SIR_Nonmeningitis",
            "AMO_MIC": "WGS_AMO",
            "AMO_Res": "WGS_AMO_SIR", 
            "MER_MIC": "WGS_MER", 
            "MER_Res": "WGS_MER_SIR",
            "TAX_MIC": "WGS_TAX", 
            "TAX_Res(Meningital)": "WGS_TAX_SIR_Meningitis", 
            "TAX_Res(Non-meningital)": "WGS_TAX_SIR_Nonmeningitis",
            "CFT_MIC": "WGS_CFT",  
            "CFT_Res(Meningital)": "WGS_CFT_SIR_Meningitis",
            "CFT_Res(Non-meningital)": "WGS_CFT_SIR_Nonmeningitis",
            "CFX_MIC": "WGS_CFX",
            "CFX_Res": "WGS_CFX_SIR",
            "ERY_MIC": "WGS_ERY",
            "ERY_Res": "WGS_ERY_SIR",
            "CLI_MIC": "WGS_CLI",
            "CLI_Res": "WGS_CLI_SIR",
            "ERY_CLI_Res": "WGS_ERY_CLI",
            "COT_MIC": "WGS_COT",
            "COT_Res": "WGS_COT_SIR",
            "TET_MIC": "WGS_TET",
            "TET_Res": "WGS_TET_SIR",
            "DOX_MIC": "WGS_DOX",
            "DOX_Res": "WGS_DOX_SIR",
            "LFX_MIC": "WGS_LFX",
            "LFX_Res": "WGS_LFX_SIR",
            "CHL_MIC": "WGS_CHL",
            "CHL_Res": "WGS_CHL_SIR",
            "RIF_MIC": "WGS_RIF",
            "RIF_Res": "WGS_RIF_SIR",
            "VAN_MIC": "WGS_VAN",
            "VAN_Res": "WGS_VAN_SIR",
        },
        inplace=True
    )

    # Check all GPSCs have colours assigned, then assign those colours
    # No GPSC assignment as TRANSPARENT
    dict_gpsc_colour = df_gpsc_colour.set_index("GPSC")["GPSC__colour"].to_dict()
    dict_gpsc_colour["_"] = "TRANSPARENT"
    if gpsc_no_colour := (set(df_table3_new_data["GPSC"]) - set(dict_gpsc_colour)):
        sys.exit(f"Error: The following GPSC(s) are not found in the selected GPSC colour assignment file: {', '.join(sorted(gpsc_no_colour))}")
    df_table3_new_data["GPSC__colour"] = df_table3_new_data["GPSC"].map(dict_gpsc_colour)

    # Strip leading 0 in serotype
    # Check all serotypes have colours assigned, then assign those colours 
    df_table3_new_data["In_silico_serotype"] = df_table3_new_data["In_silico_serotype"].str.replace(r"^0+", "", regex=True)
    if serotype_no_colour := (set(df_table3_new_data["In_silico_serotype"]) - set(df_serotype_colour["In_silico_serotype"])):
        sys.exit(f"Error: The following serotype(s) are not found in the selected serotype colour assignment file: {', '.join(sorted(serotype_no_colour))}")
    df_table3_new_data["In_silico_serotype__colour"] = df_table3_new_data["In_silico_serotype"].map(df_serotype_colour.set_index("In_silico_serotype")["In_silico_serotype__colour"])

    # Remove remove spaces and duplicated NF (happen in PBP AMR), and fill empty as _ in WGS columns
    for col in df_table3_new_data.columns:
        if col.startswith("WGS_") and "_SIR" not in col:
            df_table3_new_data[col] = df_table3_new_data[col].str.replace(" ", "").str.replace(r"^(NF){2,}$", "NF", regex=True).str.replace(r"^$", "_", regex=True)
    
    # Generate Cot based on COT_Determinant with table3 format and LOW COVERAGE warnings removed
    def cot_format_convert(determinants):
        ret = []

        for determinant in determinants.split("; "):
            if "LOW COVERAGE" in determinant:
                continue
            if determinant.startswith("FOLA"):
                ret.append(f"FOLA_{determinant.split(' ')[-1]}")
            elif determinant.startswith("FOLP"):
                ret.append(f"FOLP_{determinant.split(' ')[-1]}_{determinant.split(' ')[-3]}")
            
        return ":".join(ret) if ret else "NEG"
    
    df_table3_new_data["Cot"] = df_table3_new_data["COT_Determinant"].apply(cot_format_convert)

    # Generate Tet__autocolour based on TET_Determinant with table3 format
    def tet_format_convert(determinants):
        ret = set(determinant.split("_")[0] for determinant in determinants.split("; ") if determinant != "_")
        return ":".join(ret) if ret else "NEG"
    
    df_table3_new_data["Tet__autocolour"] = df_table3_new_data["TET_Determinant"].apply(tet_format_convert)

    # Generate PBP1A_2B_2X__autocolour based on existing columns
    df_table3_new_data["PBP1A_2B_2X__autocolour"] = df_table3_new_data["pbp1a"] + "__" + df_table3_new_data["pbp2b"] + "__" + df_table3_new_data["pbp2x"]

    # Extract and reorder relevant columns
    # No_of_genome and Duplicate columns will be inserted in integrate_table3 function
    df_table3_new_data = df_table3_new_data[[
        "Lane_id", "Public_name", "Sanger_sample_id", "ERR", "ERS", 
        "In_silico_ST", "aroE", "gdh", "gki", "recP", "spi", "xpt", "ddl", 
        "GPSC", "GPSC__colour", 
        "In_silico_serotype", "In_silico_serotype__colour", 
        "pbp1a", "pbp2b", "pbp2x", 
        "WGS_PEN", "WGS_PEN_SIR_Meningitis", "WGS_PEN_SIR_Nonmeningitis", 
        "WGS_AMO", "WGS_AMO_SIR", 
        "WGS_MER", "WGS_MER_SIR", 
        "WGS_TAX", "WGS_TAX_SIR_Meningitis", "WGS_TAX_SIR_Nonmeningitis", 
        "WGS_CFT", "WGS_CFT_SIR_Meningitis", "WGS_CFT_SIR_Nonmeningitis", 
        "WGS_CFX", "WGS_CFX_SIR", 
        "WGS_ERY", "WGS_ERY_SIR", 
        "WGS_CLI", "WGS_CLI_SIR", 
        "WGS_SYN", "WGS_SYN_SIR", 
        "WGS_LZO", "WGS_LZO_SIR", 
        "WGS_ERY_CLI", 
        "WGS_COT", "WGS_COT_SIR", 
        "WGS_TET", "WGS_TET_SIR", 
        "WGS_DOX", "WGS_DOX_SIR", 
        "WGS_LFX", "WGS_LFX_SIR", 
        "WGS_CHL", "WGS_CHL_SIR", 
        "WGS_RIF", "WGS_RIF_SIR", 
        "WGS_VAN", "WGS_VAN_SIR", 
        "EC", 
        "Cot", 
        "Tet__autocolour", 
        # "FQ__autocolour", 
        # "Other", 
        "PBP1A_2B_2X__autocolour", 
        # "WGS_PEN_SIR_Meningitis__col our", "WGS_PEN_SIR_Nonmeningitis__colour", "WGS_AMO_SIR__colour", "WGS_MER_SIR__colour", "WGS_TAX_SIR_Meningitis__colour", "WGS_TAX_SIR_Nonmeningitis__colour", "WGS_CFT_SIR_Meningitis__colour", "WGS_CFT_SIR_Nonmeningitis__colour", "WGS_CFX_SIR__colour", "WGS_ERY_SIR__colour", "WGS_CLI_SIR__colour", "WGS_SYN_SIR__colour", "WGS_LZO_SIR__colour", "WGS_COT_SIR__colour", "WGS_TET_SIR__colour", "WGS_DOX_SIR__colour", "WGS_LFX_SIR__colour", "WGS_CHL_SIR__colour", "WGS_RIF_SIR__colour", "WGS_VAN_SIR__colour", 
        # "ermB", "ermB__colour", 
        # "mefA", "mefA__colour", 
        # "folA_I100L", "folA_I100L__colour", 
        # "folP__autocolour", 
        # "cat", "cat__colour"
    ]]



    # TODO - Remove test line when done
    df_table3_new_data.to_csv("table3_test.csv", index=False)    

    return df_table3_new_data


def integrate_table2(df_table2_new_data, df_table2, table2_path):
    # Ensure new Lane_id(s) do not exist in the existing table2
    if already_exist_lane_id := set(df_table2["Lane_id"]).intersection(df_table2_new_data["Lane_id"]):
        sys.exit(f"Error: The following Lane_ID(s) already exist in {table2_path}: {', '.join(sorted(already_exist_lane_id))}.")

    pd.concat([df_table2, df_table2_new_data], axis=0).to_csv(table2_path, index=False)


def integrate_table3(df_table3_new_data, df_table3, table3_path):
    # TODO - To be completed
    pass


def parse_arguments():
    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description='Integrate GPS Pipeline results into gps2-data compatible table2.csv and table3.csv',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    base_path = os.path.abspath(os.path.dirname(sys.argv[0]))

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

    parser.add_argument(
        '-g', '--gpsccolour',
        default=os.path.join(base_path, "data/gpsc_colours.csv"),
        help='path to GPSC colour assignment file'
    )
    
    parser.add_argument(
        '-s', '--serotypecolour',
        default=os.path.join(base_path, "data/serotype_colours.csv"),
        help='path to serotype colour assignment file'
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()