# This module contains 'validate' function and its supporting functions.
# 'validate' function takes path to GPS database, then check whether the tables contain only the expected columns
# and all fields contain only the expected values or values in the expected formats for their respective columns. 


import pandas as pd
import sys
import os
import re
from datetime import date
import bin.config as config


# The main function to perform validation on the provided GPS1 database tables.
def validate(path, version, check=False):
    global FOUND_ERRORS
    FOUND_ERRORS = False

    global UPDATED_CASE 
    UPDATED_CASE = set()

    global STRIPPED_WHITESPACE
    STRIPPED_WHITESPACE = set()

    global INSERTED_METADATA
    INSERTED_METADATA = set()

    config.LOG.info(f'Loading the tables at {path} now...')

    table1, table2, table3 = (os.path.join(path, table) for table in ("table1.csv", "table2.csv", "table3.csv"))

    df_index = read_tables(table1, table2, table3)

    config.LOG.info(f'Validating {table1} now...')
    check_meta_table(df_index[table1], table1, version)

    config.LOG.info(f'Validating {table2} now...')
    check_qc_table(df_index[table2], table2, version)

    config.LOG.info(f'Validating {table3} now...')
    check_analysis_table(df_index[table3], table3, version)

    if version == 2:
        config.LOG.info(f'Cross-checking {table1} and {table3} now...')
        add_unique_repeat_to_metadata(df_index, table1, table3)

        config.LOG.info(f'Cross-checking {table2} and {table3} now...')
        crosscheck_public_name(df_index[table2], table2, df_index[table3], table3)
        crosscheck_qc_and_insilico(df_index[table2], table2, df_index[table3], table3)

    # If not in check mode, and there is a case conversion, whitespace stripping, or repeat addition, save the result
    if not check:
        for table in sorted(UPDATED_CASE | STRIPPED_WHITESPACE | INSERTED_METADATA):
            df_index[table].to_csv(table, index=False)
            if table in UPDATED_CASE:
                config.LOG.info(f'The unexpected lowercase value(s) in {table} have been fixed in-place.')
            if table in STRIPPED_WHITESPACE:
                config.LOG.info(f'The leading/trailing whitespace(s) in value(s) in {table} have been fixed in-place.')
            if table in INSERTED_METADATA:
                config.LOG.info(f'The missing repeat(s) which marked as UNIQUE and have their original(s) available have been inserted into {table} based on their original(s).')

    if FOUND_ERRORS:
        config.LOG.error(f'The validation of the tables at {path} completed with error(s). The process will now be halted. Please correct the error(s) and re-run the processor')
        sys.exit(1)
    else:
        config.LOG.info(f'The validation of the tables at {path} completed without error.')


# Read the tables into Pandas dataframes for processing
def read_tables(table1, table2, table3):
    df_index = dict()
    for table in table1, table2, table3:
        df_index[table] = pd.read_csv(table, dtype=str, keep_default_na=False)
    return df_index


# Check whether meta table only contains expected values / patterns
def check_meta_table(df_meta, table, version):
    match version:
        case 1:
            meta_columns = ["Sample_name", "Public_name", "Study_name", "Selection_random", "Continent", "Country", "Region", "City", "Facility_where_collected", "Submitting_institution", "Month", "Year", "Gender", "Age_years", "Age_months", "Age_days", "Clinical_manifestation", "Source", "HIV_status", "Underlying_conditions", "Phenotypic_serotype_method", "Phenotypic_serotype", "Sequence_Type", "aroE", "gdh", "gki", "recP", "spi", "xpt", "ddl", "AST_method_Penicillin", "Penicillin", "AST_method_Amoxicillin", "Amoxicillin", "AST_method_Cefotaxime", "Cefotaxime", "AST_method_Ceftriaxone", "Ceftriaxone", "AST_method_Cefuroxime", "Cefuroxime", "AST_method_Meropenem", "Meropenem", "AST_method_Erythromycin", "Erythromycin", "AST_method_Clindamycin", "Clindamycin", "AST_method_COT", "COT", "AST_method_Vancomycin", "Vancomycin", "AST_method_Linezolid", "Linezolid", "AST_method_Ciprofloxacin", "Ciprofloxacin", "AST_method_Chloramphenicol", "Chloramphenicol", "AST_method_Tetracycline", "Tetracycline", "AST_method_Levofloxacin", "Levofloxacin", "AST_method_Synercid", "Synercid", "AST_method_Rifampin", "Rifampin", "Comments"]
            antibiotics_columns = ['Penicillin', 'Amoxicillin', 'Cefotaxime', 'Ceftriaxone', 'Cefuroxime', 'Meropenem', 'Erythromycin', 'Clindamycin', 'COT', 'Vancomycin', 'Linezolid', 'Ciprofloxacin', 'Chloramphenicol', 'Tetracycline', 'Levofloxacin', 'Synercid', 'Rifampin']
            check_case_only_columns = ['Study_name', 'Region', 'City', 'Facility_where_collected', 'Submitting_institution', 'Underlying_conditions', 'Phenotypic_serotype_method', 'AST_method_Penicillin', 'AST_method_Amoxicillin', 'AST_method_Cefotaxime', 'AST_method_Ceftriaxone', 'AST_method_Cefuroxime', 'AST_method_Meropenem', 'AST_method_Erythromycin', 'AST_method_Clindamycin', 'AST_method_COT', 'AST_method_Vancomycin', 'AST_method_Linezolid', 'AST_method_Ciprofloxacin', 'AST_method_Chloramphenicol', 'AST_method_Tetracycline', 'AST_method_Levofloxacin', 'AST_method_Synercid', 'AST_method_Rifampin']
        case 2:
            meta_columns = ["Sample_name", "Public_name", "Study_name", "Selection_random", "Country", "Region", "City", "Facility_where_collected", "Submitting_institution", "Month", "Year", "Gender", "Age_years", "Age_months", "Age_days", "Clinical_manifestation", "Source", "HIV_status", "Underlying_conditions", "Phenotypic_serotype_method", "Phenotypic_serotype", "Sequence_Type", "aroE", "gdh", "gki", "recP", "spi", "xpt", "ddl", "AST_method_Penicillin", "Penicillin", "AST_method_Amoxicillin", "Amoxicillin", "AST_method_Cefotaxime", "Cefotaxime", "AST_method_Ceftriaxone", "Ceftriaxone", "AST_method_Cefuroxime", "Cefuroxime", "AST_method_Meropenem", "Meropenem", "AST_method_Erythromycin", "Erythromycin", "AST_method_Clindamycin", "Clindamycin", "AST_method_COT", "COT", "AST_method_Vancomycin", "Vancomycin", "AST_method_Linezolid", "Linezolid", "AST_method_Ciprofloxacin", "Ciprofloxacin", "AST_method_Chloramphenicol", "Chloramphenicol", "AST_method_Tetracycline", "Tetracycline", "AST_method_Levofloxacin", "Levofloxacin", "AST_method_Synercid", "Synercid", "AST_method_Rifampin", "Rifampin", "AST_method_Oxacillin", "Oxacillin", "Comments", "Accession_number"]
            antibiotics_columns = ['Penicillin', 'Amoxicillin', 'Cefotaxime', 'Ceftriaxone', 'Cefuroxime', 'Meropenem', 'Erythromycin', 'Clindamycin', 'COT', 'Vancomycin', 'Linezolid', 'Ciprofloxacin', 'Chloramphenicol', 'Tetracycline', 'Levofloxacin', 'Synercid', 'Rifampin', 'Oxacillin']
            check_case_only_columns = ['Study_name', 'Region', 'City', 'Facility_where_collected', 'Submitting_institution', 'Underlying_conditions', 'Phenotypic_serotype_method', 'AST_method_Penicillin', 'AST_method_Amoxicillin', 'AST_method_Cefotaxime', 'AST_method_Ceftriaxone', 'AST_method_Cefuroxime', 'AST_method_Meropenem', 'AST_method_Erythromycin', 'AST_method_Clindamycin', 'AST_method_COT', 'AST_method_Vancomycin', 'AST_method_Linezolid', 'AST_method_Ciprofloxacin', 'AST_method_Chloramphenicol', 'AST_method_Tetracycline', 'AST_method_Levofloxacin', 'AST_method_Synercid', 'AST_method_Rifampin', 'AST_method_Oxacillin', 'Accession_number']

    check_columns(df_meta, meta_columns, table)

    check_whitespace(df_meta, table)

    if version == "1":
        check_continent(df_meta, 'Continent', table)

    check_sample_name(df_meta, 'Sample_name', table)
    check_public_name(df_meta, 'Public_name', table)
    check_selection_random(df_meta, 'Selection_random', table)
    check_country(df_meta, 'Country', table)
    check_month_collection(df_meta, 'Month', table)
    check_year_collection(df_meta, 'Year', table)
    check_gender(df_meta, 'Gender', table)
    check_age_years(df_meta, 'Age_years', table)
    check_age_months(df_meta, 'Age_months', table)
    check_age_days(df_meta, 'Age_days', table)
    check_hiv_status(df_meta, 'HIV_status', table)
    check_clinical_manifestation_and_source(df_meta, 'Clinical_manifestation', 'Source', table)
    check_phenotypic_serotype(df_meta, 'Phenotypic_serotype', table)
    check_sequence_type(df_meta, 'Sequence_Type', table)

    mlst_genes_columns = ['aroE', 'gdh', 'gki', 'recP', 'spi', 'xpt', 'ddl']
    for col in mlst_genes_columns:
        check_mlst_gene(df_meta, col, table)

    for col in antibiotics_columns:
        check_antibiotic_ast(df_meta, col, table)

    for col in check_case_only_columns:
        check_case(df_meta, col, table)


# Check whether qc table only contains expected values / patterns
def check_qc_table(df_qc, table, version):
    match version:
        case 1:
            qc_columns = ["Lane_id", "Streptococcus_pneumoniae", "Total_length", "No_of_contigs", "Genome_covered", "Depth_of_coverage", "Proportion_of_Het_SNPs", "QC", "Supplier_name", "Hetsites_50bp"]
        case 2:
            qc_columns = ["Lane_id", "Public_name", "Assembler", "Streptococcus_pneumoniae", "Total_length", "No_of_contigs", "Genome_covered", "Depth_of_coverage", "Proportion_of_Het_SNPs", "QC", "Supplier_name", "Hetsites_50bp"]

    check_columns(df_qc, qc_columns, table)

    match version:
        case 1:
            check_lane_id(df_qc, 'Lane_id', table)
        case 2:
            check_case_and_space_columns = ['Lane_id', 'Public_name']
            for col in check_case_and_space_columns:
                check_case(df_qc, col, table)
                check_space(df_qc, col, table)
            check_assembler(df_qc, 'Assembler', table)

    check_lane_id_is_unqiue(df_qc, 'Lane_id', table)
    check_streptococcus_pneumoniae(df_qc, 'Streptococcus_pneumoniae', table, version)
    check_total_length(df_qc, 'Total_length', table)
    check_no_of_contigs(df_qc, 'No_of_contigs', table)
    check_genome_covered(df_qc, 'Genome_covered', table)
    check_depth_of_coverage(df_qc, 'Depth_of_coverage', table, version)
    check_proportion_of_het_snps(df_qc, 'Proportion_of_Het_SNPs', table)
    check_qc(df_qc, 'QC', table)
    check_hetsites_50bp(df_qc, 'Hetsites_50bp', table)

    check_case_only_columns = ['Supplier_name']
    for col in check_case_only_columns:
        check_case(df_qc, col, table)


# Check whether analysis table only contains expected values / patterns
def check_analysis_table(df_analysis, table, version):
    match version:
        case 1:
            analysis_columns = ["Lane_id", "Sample", "Public_name", "ERR", "ERS", "No_of_genome", "Duplicate", "Paper_1", "In_silico_ST", "aroE", "gdh", "gki", "recP", "spi", "xpt", "ddl", "GPSC", "GPSC__colour", "In_silico_serotype", "In_silico_serotype__colour", "pbp1a", "pbp2b", "pbp2x", "WGS_PEN", "WGS_PEN_SIR_Meningitis", "WGS_PEN_SIR_Nonmeningitis", "WGS_AMO", "WGS_AMO_SIR", "WGS_MER", "WGS_MER_SIR", "WGS_TAX", "WGS_TAX_SIR_Meningitis", "WGS_TAX_SIR_Nonmeningitis", "WGS_CFT", "WGS_CFT_SIR_Meningitis", "WGS_CFT_SIR_Nonmeningitis", "WGS_CFX", "WGS_CFX_SIR", "WGS_ERY", "WGS_ERY_SIR", "WGS_CLI", "WGS_CLI_SIR", "WGS_SYN", "WGS_SYN_SIR", "WGS_LZO", "WGS_LZO_SIR", "WGS_ERY_CLI", "WGS_COT", "WGS_COT_SIR", "WGS_TET", "WGS_TET_SIR", "WGS_DOX", "WGS_DOX_SIR", "WGS_LFX", "WGS_LFX_SIR", "WGS_CHL", "WGS_CHL_SIR", "WGS_RIF", "WGS_RIF_SIR", "WGS_VAN", "WGS_VAN_SIR", "EC", "Cot", "Tet__autocolour", "FQ__autocolour", "Other", "PBP1A_2B_2X__autocolour", "WGS_PEN_SIR_Meningitis__colour", "WGS_PEN_SIR_Nonmeningitis__colour", "WGS_AMO_SIR__colour", "WGS_MER_SIR__colour", "WGS_TAX_SIR_Meningitis__colour", "WGS_TAX_SIR_Nonmeningitis__colour", "WGS_CFT_SIR_Meningitis__colour", "WGS_CFT_SIR_Nonmeningitis__colour", "WGS_CFX_SIR__colour", "WGS_ERY_SIR__colour", "WGS_CLI_SIR__colour", "WGS_SYN_SIR__colour", "WGS_LZO_SIR__colour", "WGS_COT_SIR__colour", "WGS_TET_SIR__colour", "WGS_DOX_SIR__colour", "WGS_LFX_SIR__colour", "WGS_CHL_SIR__colour", "WGS_RIF_SIR__colour", "WGS_VAN_SIR__colour", "ermB", "ermB__colour", "mefA", "mefA__colour", "folA_I100L", "folA_I100L__colour", "folP__autocolour", "cat", "cat__colour"]
        case 2:
            analysis_columns = ["Lane_id", "Sanger_sample_id", "Public_name", "ERR", "ERS", "No_of_genome", "Duplicate", "In_silico_ST", "aroE", "gdh", "gki", "recP", "spi", "xpt", "ddl", "GPSC", "GPSC__colour", "In_silico_serotype", "In_silico_serotype__colour", "pbp1a", "pbp2b", "pbp2x", "WGS_PEN", "WGS_PEN_SIR_Meningitis", "WGS_PEN_SIR_Nonmeningitis", "WGS_AMO", "WGS_AMO_SIR", "WGS_MER", "WGS_MER_SIR", "WGS_TAX", "WGS_TAX_SIR_Meningitis", "WGS_TAX_SIR_Nonmeningitis", "WGS_CFT", "WGS_CFT_SIR_Meningitis", "WGS_CFT_SIR_Nonmeningitis", "WGS_CFX", "WGS_CFX_SIR", "WGS_ERY", "WGS_ERY_SIR", "WGS_CLI", "WGS_CLI_SIR", "WGS_SYN", "WGS_SYN_SIR", "WGS_LZO", "WGS_LZO_SIR", "WGS_ERY_CLI", "WGS_COT", "WGS_COT_SIR", "WGS_TET", "WGS_TET_SIR", "WGS_DOX", "WGS_DOX_SIR", "WGS_LFX", "WGS_LFX_SIR", "WGS_CHL", "WGS_CHL_SIR", "WGS_RIF", "WGS_RIF_SIR", "WGS_VAN", "WGS_VAN_SIR", "EC", "Cot", "Tet__autocolour", "FQ__autocolour", "Other", "PBP1A_2B_2X__autocolour", "WGS_PEN_SIR_Meningitis__colour", "WGS_PEN_SIR_Nonmeningitis__colour", "WGS_AMO_SIR__colour", "WGS_MER_SIR__colour", "WGS_TAX_SIR_Meningitis__colour", "WGS_TAX_SIR_Nonmeningitis__colour", "WGS_CFT_SIR_Meningitis__colour", "WGS_CFT_SIR_Nonmeningitis__colour", "WGS_CFX_SIR__colour", "WGS_ERY_SIR__colour", "WGS_CLI_SIR__colour", "WGS_SYN_SIR__colour", "WGS_LZO_SIR__colour", "WGS_COT_SIR__colour", "WGS_TET_SIR__colour", "WGS_DOX_SIR__colour", "WGS_LFX_SIR__colour", "WGS_CHL_SIR__colour", "WGS_RIF_SIR__colour", "WGS_VAN_SIR__colour", "ermB", "ermB__colour", "mefA", "mefA__colour", "folA_I100L", "folA_I100L__colour", "folP__autocolour", "cat", "cat__colour"]

    check_columns(df_analysis, analysis_columns, table)

    match version:
        case 1:
            check_sanger_sample_id(df_analysis, 'Sample', table, version)
            check_paper_1(df_analysis, 'Paper_1', table)
            check_lane_id(df_analysis, 'Lane_id', table)
        case 2:
            check_sanger_sample_id(df_analysis, 'Sanger_sample_id', table, version)
            check_case(df_analysis, 'Lane_id', table)
            check_space(df_analysis, 'Lane_id', table)
    
    check_case(df_analysis, 'Public_name', table)
    check_space(df_analysis, 'Public_name', table)

    check_lane_id_is_unqiue(df_analysis, 'Lane_id', table)
    check_err(df_analysis, 'ERR', table, version)
    check_ers(df_analysis, 'ERS', table, version)
    check_no_of_genome(df_analysis, 'No_of_genome', table)
    check_duplicate(df_analysis, 'Duplicate', table, version)
    check_in_silico_st(df_analysis, 'In_silico_ST', table)

    mlst_genes_in_silico_columns = ['aroE', 'gdh', 'gki', 'recP', 'spi', 'xpt', 'ddl']
    for col in mlst_genes_in_silico_columns:
        check_mlst_gene_in_silico(df_analysis, col, table, version)

    check_gpsc(df_analysis, 'GPSC', table)

    match version:
        case 1:
            color_columns = ['GPSC__colour', 'In_silico_serotype__colour', 'ermB__colour', 'mefA__colour', 'folA_I100L__colour', 'cat__colour']
            color_columns_with_transparent = ['WGS_PEN_SIR_Meningitis__colour', 'WGS_PEN_SIR_Nonmeningitis__colour', 'WGS_AMO_SIR__colour', 'WGS_MER_SIR__colour', 'WGS_TAX_SIR_Meningitis__colour', 'WGS_TAX_SIR_Nonmeningitis__colour', 'WGS_CFT_SIR_Meningitis__colour', 'WGS_CFT_SIR_Nonmeningitis__colour', 'WGS_CFX_SIR__colour', 'WGS_ERY_SIR__colour', 'WGS_CLI_SIR__colour', 'WGS_SYN_SIR__colour', 'WGS_LZO_SIR__colour', 'WGS_COT_SIR__colour', 'WGS_TET_SIR__colour', 'WGS_DOX_SIR__colour', 'WGS_LFX_SIR__colour', 'WGS_CHL_SIR__colour', 'WGS_RIF_SIR__colour', 'WGS_VAN_SIR__colour']
        case 2:
            color_columns = ['ermB__colour', 'mefA__colour', 'folA_I100L__colour', 'cat__colour']
            color_columns_with_transparent = ['GPSC__colour', 'In_silico_serotype__colour', 'WGS_PEN_SIR_Meningitis__colour', 'WGS_PEN_SIR_Nonmeningitis__colour', 'WGS_AMO_SIR__colour', 'WGS_MER_SIR__colour', 'WGS_TAX_SIR_Meningitis__colour', 'WGS_TAX_SIR_Nonmeningitis__colour', 'WGS_CFT_SIR_Meningitis__colour', 'WGS_CFT_SIR_Nonmeningitis__colour', 'WGS_CFX_SIR__colour', 'WGS_ERY_SIR__colour', 'WGS_CLI_SIR__colour', 'WGS_SYN_SIR__colour', 'WGS_LZO_SIR__colour', 'WGS_COT_SIR__colour', 'WGS_TET_SIR__colour', 'WGS_DOX_SIR__colour', 'WGS_LFX_SIR__colour', 'WGS_CHL_SIR__colour', 'WGS_RIF_SIR__colour', 'WGS_VAN_SIR__colour']

    for col in color_columns:
        check_color(df_analysis, col, table)
    for col in color_columns_with_transparent:
        check_color(df_analysis, col, table, transparent=True)
    
    check_in_silico_serotype(df_analysis, 'In_silico_serotype', table)

    pbp_columns = ['pbp1a', 'pbp2b', 'pbp2x']
    for col in pbp_columns:
        check_pbp(df_analysis, col, table)

    match version:
        case 1:
            wgs_columns = ['WGS_PEN', 'WGS_AMO', 'WGS_MER', 'WGS_TAX', 'WGS_CFT', 'WGS_CFX', 'WGS_ERY', 'WGS_CLI', 'WGS_SYN', 'WGS_LZO', 'WGS_COT', 'WGS_TET', 'WGS_DOX', 'WGS_LFX', 'WGS_CHL', 'WGS_RIF', 'WGS_VAN']
            wgs_invalid_columns = []
            wgs_sir_columns = ['WGS_PEN_SIR_Meningitis', 'WGS_PEN_SIR_Nonmeningitis', 'WGS_AMO_SIR', 'WGS_MER_SIR', 'WGS_TAX_SIR_Meningitis', 'WGS_TAX_SIR_Nonmeningitis', 'WGS_CFT_SIR_Meningitis', 'WGS_CFT_SIR_Nonmeningitis', 'WGS_CFX_SIR', 'WGS_ERY_SIR', 'WGS_CLI_SIR', 'WGS_SYN_SIR', 'WGS_LZO_SIR', 'WGS_COT_SIR', 'WGS_TET_SIR', 'WGS_DOX_SIR', 'WGS_LFX_SIR', 'WGS_CHL_SIR', 'WGS_RIF_SIR', 'WGS_VAN_SIR']
            wgs_sir_invalid_columns = []
            ariba_sir_columns = []
        case 2:
            wgs_columns = ['WGS_PEN', 'WGS_AMO', 'WGS_MER', 'WGS_TAX', 'WGS_CFT', 'WGS_CFX', 'WGS_ERY', 'WGS_CLI', 'WGS_COT', 'WGS_TET', 'WGS_DOX', 'WGS_LFX', 'WGS_CHL', 'WGS_RIF', 'WGS_VAN']
            wgs_invalid_columns = ['WGS_SYN', 'WGS_LZO']
            wgs_sir_columns = ['WGS_PEN_SIR_Meningitis', 'WGS_PEN_SIR_Nonmeningitis', 'WGS_AMO_SIR', 'WGS_MER_SIR', 'WGS_TAX_SIR_Meningitis', 'WGS_TAX_SIR_Nonmeningitis', 'WGS_CFT_SIR_Meningitis', 'WGS_CFT_SIR_Nonmeningitis', 'WGS_CFX_SIR']
            wgs_sir_invalid_columns = ['WGS_SYN_SIR', 'WGS_LZO_SIR']
            ariba_sir_columns = ['WGS_ERY_SIR', 'WGS_CLI_SIR', 'WGS_COT_SIR', 'WGS_TET_SIR', 'WGS_DOX_SIR', 'WGS_LFX_SIR', 'WGS_CHL_SIR', 'WGS_RIF_SIR', 'WGS_VAN_SIR']
    for col in wgs_columns:
        check_wgs(df_analysis, col, table)
    for col in wgs_sir_columns:
        check_wgs_sir(df_analysis, col, table)
    for col in ariba_sir_columns:
        check_ariba_sir_columns(df_analysis, col, table)
    for col in wgs_invalid_columns + wgs_sir_invalid_columns:
        check_invalid_wgs_and_wgs_sir(df_analysis, col, table)
    
    check_wgs_ery_cli(df_analysis, 'WGS_ERY_CLI', table, version)
    check_pbp1a_2b_2x_autocolour(df_analysis, 'PBP1A_2B_2X__autocolour', table)

    pos_neg_columns = ['ermB', 'mefA', 'folA_I100L', 'cat']
    for col in pos_neg_columns:
        check_pos_neg(df_analysis, col, table)

    check_case_only_columns = ['Cot', 'Tet__autocolour', 'FQ__autocolour', 'folP__autocolour', 'Other']
    for col in check_case_only_columns:
        check_case(df_analysis, col, table)


# Check whether tables contain only the expected columns
def check_columns(df, columns, table):
    if (diff := set(list(df)) - set(columns)):
        config.LOG.critical(f'{table} has the following unexpected column(s): {", ".join(diff)}. Incorrect or incompatible table is used and cannot be validated. The process will now be halted.')
        sys.exit(1)
    if (diff := set(list(columns)) - set(df)):
        config.LOG.critical(f'{table} is missing the following column(s): {", ".join(diff)}. Incorrect or incompatible table is used and cannot be validated. The process will now be halted.')
        sys.exit(1)


# Check column values contain no leading or trailing whitespace; remove all leading or trailing whitespace if found
def check_whitespace(df, table):
    for column_name, column in df.items():
        stripped_column = column.str.strip()

        if column.equals(stripped_column):
            continue

        df[column_name] = stripped_column

        global STRIPPED_WHITESPACE
        STRIPPED_WHITESPACE.add(table)
        config.LOG.info(f'{column_name} in {table} contains value(s) with leading/trailing whitespace(s).')


# Check column values contain no space at any position in the string
def check_space(df, column_name, table):
    values = df[column_name].unique()
    unexpected = [v for v in values if re.search(r' ', v)]

    if len(unexpected) == 0:
        return
    
    config.LOG.error(f'{column_name} in {table} has the following value(s) with space(s): {", ".join(unexpected)}.')
    found_error()

# Check column values are in uppercase letters and have no space
def check_sample_name(df, column_name, table):
    check_case(df, column_name, table)
    check_space(df, column_name, table)

# Check column values are in uppercase letters, have no space, and unique
def check_public_name(df, column_name, table):
    check_case(df, column_name, table)
    check_space(df, column_name, table)
    
    duplicated_names = df[df.duplicated(subset=[column_name], keep=False)][column_name].unique()
    
    if len(duplicated_names) == 0:
        return
    
    config.LOG.error(f'{column_name} in {table} contains duplicate entries of the following Public_name(s): {", ".join(duplicated_names)}.')
    found_error()

# Check column values contain Y, N, _ only
def check_selection_random(df, column_name, table):
    expected = {'Y', 'N', '_'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)


# Check column values is in the expected continent only
def check_continent(df, column_name, table):
    expected = {'AFRICA', 'ASIA', 'CENTRAL AMERICA', 'EUROPE', 'LATIN AMERICA', 'NORTH AMERICA', 'OCEANIA', '_'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)


# Warn if column values contain countries not in 'data/pcv_introduction_year.csv'; or countries not in 'data/alpha2_country.csv'
def check_country(df, column_name, table):
    check_case(df, column_name, table)

    countries = get_uniques_non_empty(df, column_name)

    # Check vaccine info
    no_vaccine_info = set(countries) - set(config.PCV_INTRO_YEARS.keys())
    if no_vaccine_info:
        config.LOG.warning(f'{column_name} in {table} has the following country(s) without vaccine information: {", ".join(sorted(no_vaccine_info))}. If their National Immunisation/Vaccination Programme includes PCV, please add their information to {config.PCV_INTRO_YEARS_FILE}.') 

    # Check alpha2 info; database contains 'WEST AFRICA' which is not a country, therefore hard-coded for its removal
    no_alpha2 = set(countries) - set(config.COUNTRY_ALPHA2) - {'WEST AFRICA'}
    if no_alpha2:
        config.LOG.error(f'{column_name} in {table} has the following country(s) without ISO 3166-1 alpha-2 code: {", ".join(no_alpha2)}. Please check spelling or add their alpha-2 code information to {config.ALPHA2_COUNTY_FILE}.')
        found_error()


# Check column values is in the expected months only
def check_month_collection(df, column_name, table):
    expected = {'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC', '_'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)


# Check column values is within reasonable year range
def check_year_collection(df, column_name, table):
    check_year(df, column_name, table, lo=1900)


# Check column values is in the expected genders only
def check_gender(df, column_name, table):
    expected = {'M', 'F', '_'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)


# Check column values is within reasonable age year, or in 'data/non_standard_ages.csv'
def check_age_years(df, column_name, table):
    values = get_uniques_non_empty(df, column_name)
    values = set(values) - set(config.NON_STANDARD_AGES.keys())

    unexpected = [v for v in values if not (re.match(r'^(?!0[0-9])([0-9]+([.][0-9]+)?)$', v) and 0 <= float(v) <= 130)]

    if len(unexpected) == 0:
        return

    config.LOG.error(f'{column_name} in {table} has the following unexpected value(s): {", ".join(unexpected)}. If valid, please add to {config.NON_STANDARD_AGES_FILE} and state whether it is less than 5 years old or not.')
    found_error()


# Check column values is within reasonable age month 
def check_age_months(df, column_name, table):
    check_regex(df, column_name, table, allow_empty=True, float_range=(0, 12))


# Check column values contain 0 - 31 integers or _ only 
def check_age_days(df, column_name, table):
    check_int_range(df, column_name, table, lo=0, hi=31, allow_empty=True)


# Check column values contain P, N, _ only
def check_hiv_status(df, column_name, table):
    expected = {'P', 'N', '_'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)


# Check whether the Clinical_manifestation and Source combinations are all included in the global dictionary MANIFESTATIONS
def check_clinical_manifestation_and_source(df, clinical_manifestation, source, table):
    check_case(df, clinical_manifestation, table)
    check_case(df, source, table)
    combinations = df.set_index(['Clinical_manifestation', 'Source']).index.unique().tolist()
    unexpected = set(combinations) - set(config.MANIFESTATIONS.keys())

    if len(unexpected) == 0:
        return

    unexpected = [f'{n}' for n in unexpected]
    config.LOG.error(f'{table} has the following unexpected Clinical_manifestation and Source combination(s): {", ".join(unexpected)}. Please add the combination(s) to {config.MANIFESTATIONS_FILE} and state the resulting Manifestation.')
    found_error()


# Check column values fit the phenotypic serotype pattern 
# Expect "NT" or "serotype, optionally followed by [separated by / or &] serotype or sub-group or NT"
def check_phenotypic_serotype(df, column_name, table):
    check_case(df, column_name, table)
    check_regex(df, column_name, table, allow_empty=True, absolute=False, pattern=r'^(NT|((?!0)[0-9]{1,2}[A-Z]?)((&|\/)(?!$|&|\/)((?!0)[0-9]{0,2}[A-Z]?|NT))*)$')


# Check column values contain 1 - 50000 integers or UNKNOWN, _ only
def check_sequence_type(df, column_name, table):
    check_case(df, column_name, table)
    check_int_range(df, column_name, table, lo=1, hi=50000, allow_empty=True, others=['UNKNOWN'])


# Check column values contain 1 or larger integers or UNKNOWN, _ only
def check_mlst_gene(df, column_name, table):
    check_case(df, column_name, table)
    check_int_range(df, column_name, table, lo=1, allow_empty=True, others=['UNKNOWN'], absolute=False)


# Check column values contain numeric values (can be a range: >, <, >=, <=) or I, R, S, NS, _ only 
def check_antibiotic_ast(df, column_name, table):
    check_case(df, column_name, table)
    check_regex(df, column_name, table, allow_empty=True, pattern=r'^([IRS]|NS|([<>]=?)?(?!0[0-9])([0-9]+([.][0-9]+)?))$', absolute=False)


# Check column values are in Sanger Lane ID format only
def check_lane_id(df, column_name, table):
    check_regex(df, column_name, table, pattern=r'^(?!0)[0-9]{4,5}_[1-9]#(?!0)[0-9]{1,3}$')


# Check column values are "SHOVILL" or "UNICYCLER" only
def check_assembler(df, column_name, table):
    expected = {'SHOVILL', 'UNICYCLER'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)

# Check column values are float in 0 - 100 only. Allows to be _ in GPS2 as well.
def check_streptococcus_pneumoniae(df, column_name, table, version):
    float_range = (0, 100)
    match version:
        case 1:
            check_regex(df, column_name, table, float_range=float_range)
        case 2:
            check_regex(df, column_name, table, float_range=float_range, allow_empty=True)


# Check column values contain 1 or larger integers or _ only
def check_total_length(df, column_name, table):
    check_int_range(df, column_name, table, lo=1, allow_empty=True)


# Check column values contain 1 or larger integers or _ only
def check_no_of_contigs(df, column_name, table):
    check_int_range(df, column_name, table, lo=1, allow_empty=True)


# Check column values are float in 0 - 100 or _ only
def check_genome_covered(df, column_name, table):
    check_regex(df, column_name, table, float_range=(0, 100), allow_empty=True)


# Check column values are float in 0 - infinity only. Allows to be _ in GPS2 as well.
def check_depth_of_coverage(df, column_name, table, version):
    float_range = (0, float('inf'))
    match version:
        case 1:
            check_regex(df, column_name, table, float_range=float_range)
        case 2:
            check_regex(df, column_name, table, float_range=float_range, allow_empty=True)

# Check column values are float in 0 - 100 or _ only
def check_proportion_of_het_snps(df, column_name, table):
    check_regex(df, column_name, table, float_range=(0, 100), allow_empty=True)


# Check column values contain PASS, PASSPLUS, FAIL, _ only
def check_qc(df, column_name, table):
    expected = {'PASS', 'PASSPLUS', 'FAIL', '_'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)


# Check column values contain 0 or larger integers or _ only
def check_hetsites_50bp(df, column_name, table):
    check_int_range(df, column_name, table, lo=0, allow_empty=True)


# Check column values are in Sanger sample format only. Allows to be _ in GPS2 as well.
def check_sanger_sample_id(df, column_name, table, version):
    check_case(df, column_name, table)
    pattern=r'^[0-9]{4}STDY[0-9]{7,8}$'
    match version:
        case 1:
            check_regex(df, column_name, table, pattern=pattern)
        case 2:
            check_regex(df, column_name, table, pattern=pattern, allow_empty=True)

# Check column values are in valid run accession format only. Allows to be _ in GPS2 as well.
def check_err(df, column_name, table, version):
    check_case(df, column_name, table)

    match version:
        case 1:
            pattern = r'^(NOTFOUND|[ESD]RR[0-9]{6,8})$'
        case 2:
            pattern = r'^(NOTFOUND|[ESD]RR[0-9]{6,8}|_)$'

    check_regex(df, column_name, table, pattern=pattern)


# Check column values are in valid sample accession format only. Allows to be _ in GPS2 as well.
def check_ers(df, column_name, table, version):
    check_case(df, column_name, table)
    
    match version:
        case 1:
            pattern = r'^[ESD]RS[0-9]{6,8}$'
        case 2:
            pattern = r'^([ESD]RS[0-9]{6,8}|_)$'

    check_regex(df, column_name, table, pattern=pattern)


# Check column values contain 1 - 4 integers only
def check_no_of_genome(df, column_name, table):
    check_int_range(df, column_name, table, lo=1, hi=4)


# Check column values contain DUPLICATE, UNIQUE only
# Each public name should contains one UNIQUE value at most
def check_duplicate(df, column_name, table, version):
    expected = {'DUPLICATE', 'UNIQUE'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)

    df_copy = df.copy()

    match version:
        case 1:
            df_copy['Public_name_no_suffix'] = df_copy['Public_name']
            unique_public_name_string = "unique Public_name(s)"
            duplicate_string = "duplicates"
        case 2:
            df_copy['Public_name_no_suffix'] = df_copy['Public_name'].str.replace(r'_R[1-9]$', '', regex=True)
            unique_public_name_string = "unique Public_name(s) (_R* suffix repeats considered)"
            duplicate_string = "duplicates (including _R* suffix repeats)"

    df_uniques = df_copy[~df_copy['Public_name_no_suffix'].duplicated(keep=False)]
    df_duplicates = df_copy[df_copy['Public_name_no_suffix'].duplicated(keep=False)]

    uniques_as_duplicate = df_uniques[df_uniques[column_name]=='DUPLICATE']['Public_name'].tolist()
    if uniques_as_duplicate:
        config.LOG.warning(f'{table} has the following {unique_public_name_string} marked as DUPLICATE in {column_name}: {", ".join(uniques_as_duplicate)}. Please check if they are correct.')

    duplicates_no_unique = set(df_duplicates['Public_name_no_suffix']) - set(df_duplicates[df_duplicates['Duplicate']=='UNIQUE']['Public_name_no_suffix'])
    if duplicates_no_unique:
        config.LOG.warning(f'{table} has the following duplicated Public_name(s) with none of their {duplicate_string} marked as UNIQUE in {column_name}: {", ".join(duplicates_no_unique)}. Please check if they are correct.')
    
    df_duplicates_unique_count = df_duplicates[df_duplicates['Duplicate']=='UNIQUE'].groupby(['Public_name_no_suffix']).size()
    duplicates_more_than_one_unique = df_duplicates_unique_count.index[df_duplicates_unique_count > 1].tolist()
    if duplicates_more_than_one_unique:
        config.LOG.error(f'{table} has the following duplicated Public_name(s) with more than one of their {duplicate_string} marked as UNIQUE in {column_name}: {", ".join(duplicates_more_than_one_unique)}.')
        found_error()


# Check column values contain YES, NO only
def check_paper_1(df, column_name, table):
    expected = {'YES', 'NO'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)


# Check column values contain 1 - 20000 integers or NEW, -, _ only
def check_in_silico_st(df, column_name, table):
    check_case(df, column_name, table)
    check_int_range(df, column_name, table, lo=1, hi=20000, allow_empty=True, others=['NEW', '-'])


# Check column values contain: For GPS1, 1 or larger integers or NEW, PARTIAL_DELETION, ABSENT, _ only; For GPS2, all possible mlst output
def check_mlst_gene_in_silico(df, column_name, table, version):
    match version:
        case 1:
            check_case(df, column_name, table)
            check_int_range(df, column_name, table, lo=1, allow_empty=True, others=['NEW', 'PARTIAL_DELETION', 'ABSENT'])
        case 2:
            check_regex(df, column_name, table, pattern=r'^(((~?[0-9]+|[0-9]+\?)(,(~?[0-9]+|[0-9]+\?))*)|-)$')

# Check column values contain 1 - 2000 integers, _ only
def check_gpsc(df, column_name, table):
    check_int_range(df, column_name, table, lo=1, hi=2000, allow_empty=True)


# Check column values are in HEX color format, _ (optionally TRANSPARENT) only
def check_color(df, column_name, table, transparent=False):
    check_case(df, column_name, table)
    if transparent:
        check_regex(df, column_name, table, pattern=r'^(TRANSPARENT|_|#[0-9A-F]{6})$')
    else:
        check_regex(df, column_name, table, pattern=r'^(_|#[0-9A-F]{6})$')


# Check column values fit the in silico serotype pattern 
# Expect single serotype
def check_in_silico_serotype(df, column_name, table):
    check_case(df, column_name, table)
    check_regex(df, column_name, table, absolute=False, pattern=r'^((?!0)([0-9]{1,2})[A-Z]?(\/\2[A-Z])*|POSSIBLE 6[A-Z]|6E\(6A\)|6E\(6B\)|23B1|SEROGROUP 24|19AF|SWISS_NT|ALTERNATIVE_ALIB_NT|UNTYPABLE|COVERAGE TOO LOW|SEROBA FAILURE)$')


# Check column values contain 1 - 1000 integers or NEW, NF, ERROR only
def check_pbp(df, column_name, table):
    check_case(df, column_name, table)
    check_int_range(df, column_name, table, lo=0, hi=1000, others=['NEW', 'NF', 'ERROR'])


# Check column values are numeric values (can be a range: >, <, >=, <=, or value - value), FLAG, NF, or NA only
def check_wgs(df, column_name, table):
    check_case(df, column_name, table)
    check_regex(df, column_name, table, allow_empty=True, pattern=r'^(NA|FLAG|NF|([<>]=?)?(?!0[0-9])([0-9]+([.][0-9]+)?)|(?!0[0-9])([0-9]+([.][0-9]+)?)-(?!0[0-9])([0-9]+([.][0-9]+)?))$')


# Check column values contain NF, R, I, S, FLAG, _ only
def check_wgs_sir(df, column_name, table):
    expected = {'NF', 'R', 'I', 'S', 'FLAG', '_'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)

# Check column values contain S, I, R, INDETERMINABLE only
def check_ariba_sir_columns(df, column_name, table):
    expected = {'S', 'I', 'R', 'INDETERMINABLE'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)

# Check column values _ only
def check_invalid_wgs_and_wgs_sir(df, column_name, table):
    expected = {'_'}
    check_expected(df, column_name, table, expected)

# Check column values contain FLAG, NEG, POS only for GPS1, and contain 
def check_wgs_ery_cli(df, column_name, table, version):
    match version:
        case 1:
            expected = {'FLAG', 'NEG', 'POS'}
        case 2:
            expected = {'R', 'S'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)


# Check column values are in the format of x__x__x, where x is NEW, NF, 0-999, ERROR only
def check_pbp1a_2b_2x_autocolour(df, column_name, table):
    check_case(df, column_name, table)
    check_regex(df, column_name, table, allow_empty=True, pattern=r'^(NEW|NF|ERROR|(?!0[0-9])[0-9]{1,3})__(NEW|NF|ERROR|(?!0[0-9])[0-9]{1,3})__(NEW|NF|ERROR|(?!0[0-9])[0-9]{1,3})$')


# Check column values contain POS, NEG only
def check_pos_neg(df, column_name, table):
    expected = {'POS', 'NEG'}
    check_case(df, column_name, table)
    check_expected(df, column_name, table, expected)


# Check column values against expected values; report unexpected value(s) if there is any 
def check_expected(df, column_name, table, expected, absolute=True):
    extras = set(df[column_name].unique()) - expected
    
    if len(extras) == 0:
        return
    
    if absolute:
        config.LOG.error(f'{column_name} in {table} has the following unexpected value(s): {", ".join(extras)}.')
        found_error()
    else:
        config.LOG.warning(f'{column_name} in {table} has the following previously unknown value(s): {", ".join(extras)}. Please check if they are correct.')


# Check whether all strings are uppercase in the selected column, ignore values without alphabets; convert all strings to upper if any lowercase found
def check_case(df, column_name, table):
    uniques_with_alphabets = (unique for unique in df[column_name].unique() if re.search(r'[a-zA-Z]', unique))
    
    if all(unique.isupper() for unique in uniques_with_alphabets):
        return

    df[column_name] = df[column_name].str.upper()

    global UPDATED_CASE
    UPDATED_CASE.add(table)
    config.LOG.info(f'{column_name} in {table} contains lowercase value(s) while being a UPPERCASE-only column.')


# Get uniques values in a column, excluding '_'
def get_uniques_non_empty(df, column_name):
    return [unique for unique in df[column_name].unique() if unique != '_']


# Check column values contain integers in specific range (if no hi value is provided, it assume no upper limit) or values in the others list only
def check_int_range(df, column_name, table, lo, hi=float('inf'), others=None, allow_empty=False, absolute=True):
    if allow_empty:
        values = get_uniques_non_empty(df, column_name)
    else:
        values = df[column_name].unique()
    
    if others is not None:
        values = set(values) - set(others)

    unexpected = [v for v in values if (not v.isdecimal() or not lo <= int(v) <= hi)]
    
    if len(unexpected) == 0:
        return
    
    if absolute:
        config.LOG.error(f'{column_name} in {table} has the following unexpected value(s): {", ".join(unexpected)}.')
        found_error()
    else:
        config.LOG.warning(f'{column_name} in {table} has the following non-standard value(s): {", ".join(unexpected)}. Please check if they are correct.')


# Check column values is between specific year and now or _
def check_year(df, column_name, table, lo):
    check_int_range(df, column_name, table, lo=lo, hi=date.today().year, allow_empty=True)


# Check regex pattern or float range, optional: allow empty, float range, no alphabet only numeric, absolute
def check_regex(df, column_name, table, pattern=None, allow_empty=False, float_range=None, absolute=True):
    if allow_empty:
        values = get_uniques_non_empty(df, column_name)
    else:
        values = df[column_name].unique()
    
    if float_range:
        unexpected = [v for v in values if not (re.match(r'^(?!0[0-9])([0-9]+([.][0-9]+)?)$', v) and float_range[0] <= float(v) <= float_range[1])]
    else:
        unexpected = [v for v in values if not re.match(pattern, v)]

    if len(unexpected) == 0:
        return

    if absolute:
        config.LOG.error(f'{column_name} in {table} has the following unexpected value(s): {", ".join(unexpected)}.')
        found_error()
    else:
        config.LOG.warning(f'{column_name} in {table} has the following non-standard value(s): {", ".join(unexpected)}. Please check if they are correct.')


# If there is a repeat (Public_name with _R* suffix) marked as UNIQUE in Duplicate but does not exist in table1, attempt to create an entry in table1 based on its original metadata
def add_unique_repeat_to_metadata(df_index, table1, table3):
    df_meta = df_index[table1]
    df_analysis = df_index[table3]
    repeats_to_add = set(df_analysis[(df_analysis['Duplicate'] == 'UNIQUE') & (df_analysis['Public_name'].str.contains(r'_R[1-9]$'))]['Public_name']) - set(df_meta['Public_name'])

    repeats_inserted = []
    repeats_without_original = []

    for repeat in repeats_to_add:
        original = re.search(r'^(.+)_R[1-9]$', repeat).group(1)
        df_original = df_meta[df_meta['Public_name'] == original]
        if not df_original.empty:         
            row_to_insert_index = df_original.index[0]
            row_to_insert = df_meta.iloc[row_to_insert_index].copy()
            row_to_insert['Public_name'] = repeat
            df_meta = pd.concat([df_meta.iloc[:row_to_insert_index + 1], pd.DataFrame([row_to_insert]), df_meta.iloc[row_to_insert_index + 1:]]).reset_index(drop=True)
            df_index[table1] = df_meta

            global INSERTED_METADATA
            INSERTED_METADATA.add(table1)

            repeats_inserted.append(repeat)
        else:
            repeats_without_original.append(repeat)

    if repeats_inserted:
        config.LOG.info(f'The following repeats which marked as UNIQUE in {table3} are not in {table1}, but their original(s) are: {", ".join(repeats_inserted)}')
    if repeats_without_original:
        config.LOG.warning(f'The following repeats which marked as UNIQUE in {table3} are not in {table1}, nor their original(s): {", ".join(repeats_without_original)}')


# Check if Public_names in table2 and table3 are the same for the same Lane_id
def crosscheck_public_name(df_table2, table2, df_table3, table3):
    df_merged = df_table2[['Lane_id', 'Public_name']].merge(df_table3[['Lane_id', 'Public_name']], on='Lane_id', suffixes=('_table2', '_table3'))
    laneids_different_public_name = df_merged[df_merged['Public_name_table2'] != df_merged['Public_name_table3']]['Lane_id'].unique().tolist()

    if laneids_different_public_name:
        config.LOG.error(f'The following Lane_id(s) have different Public_name(s) in {table2} and {table3}: {", ".join(sorted(laneids_different_public_name))}.')
        found_error()


# Check that table3 is a subset of table2, and all and only genomes passed QC in table2 should be in table3
def crosscheck_qc_and_insilico(df_table2, table2, df_table3, table3):
    set_table2_laneid = set(df_table2["Lane_id"])
    set_table2_laneid_passed = set(df_table2[df_table2["QC"] == "PASS"]["Lane_id"])
    set_table2_laneid_failed  = set(df_table2[df_table2["QC"] == "FAIL"]["Lane_id"])
    set_table3_laneid = set(df_table3["Lane_id"])

    if (set_laneid_table3_only := set_table3_laneid - set_table2_laneid):
        config.LOG.error(f'The following Lane_id(s) are found in {table3} but not {table2}: {", ".join(sorted(set_laneid_table3_only))}.')
        found_error()

    if (set_table3_missing_passed_laneid := set_table2_laneid_passed - set_table3_laneid):
        config.LOG.error(f'The following QC passed Lane_id(s) are missing in {table3}: {", ".join(sorted(set_table3_missing_passed_laneid))}.')
        found_error()

    if (set_table3_failed_laneid := set_table2_laneid_failed.intersection(set_table3_laneid)):
        config.LOG.error(f'The following QC failed Lane_id(s) are found in {table3}: {", ".join(sorted(set_table3_failed_laneid))}.')
        found_error()


# Check if Lane_ids are unique
def check_lane_id_is_unqiue(df, column_name, table):
    duplicated_lane_ids = df[df[column_name].duplicated()][column_name].tolist()

    if duplicated_lane_ids:
        config.LOG.error(f'The following Lane_id(s) are duplicated in {table}: {", ".join(sorted(duplicated_lane_ids))}.')
        found_error()


def found_error():
    global FOUND_ERRORS
    FOUND_ERRORS = True