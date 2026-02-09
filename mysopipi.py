import streamlit as st
st.set_page_config(
    page_title="Mysopipi",
    page_icon="📊",
    layout="wide"
)
from datetime import datetime
import pandas as pd
import numpy as np
import io
import time
import re
from rapidfuzz import fuzz
from openpyxl import load_workbook

# ============================================
# FUNGSI-FUNGSI UTAMA (DARI REKAPANKU.PY)
# ============================================

def get_pretty_date_range(start_date, end_date):
    try:
        dt_start = pd.to_datetime(start_date)
        dt_end = pd.to_datetime(end_date)
        
        bulan_indo = ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]
        
        d1, m1, y1 = dt_start.day, dt_start.month, dt_start.year
        d2, m2, y2 = dt_end.day, dt_end.month, dt_end.year
        
        if y1 != y2:
            return f"{d1} {bulan_indo[m1-1]} {y1} - {d2} {bulan_indo[m2-1]} {y2}"
        elif m1 != m2:
            return f"{d1} {bulan_indo[m1-1]} - {d2} {bulan_indo[m2-1]} {y2}"
        else:
            return f"{d1} - {d2} {bulan_indo[m1-1]} {y2}"
    except:
        return ""

def clean_and_convert_to_numeric(column):
    """Menghapus semua karakter non-digit (kecuali titik dan minus) dan mengubah kolom menjadi numerik."""
    if column.dtype == 'object':
        column = column.astype(str).str.replace(r'[^\d,\-]', '', regex=True)
        column = column.str.replace(',', '.', regex=False)
    return pd.to_numeric(column, errors='coerce').fillna(0)

def clean_order_all_numeric(column):
    """Fungsi khusus untuk membersihkan kolom di file order-all."""
    cleaned_column = column.astype(str).str.replace(r'\D', '', regex=True)
    return pd.to_numeric(cleaned_column, errors='coerce').fillna(0)

def clean_columns(df):
    """Menghapus spasi di awal dan akhir dari semua nama kolom DataFrame."""
    df.columns = df.columns.str.strip()
    return df

def extract_relevant_variation_part(var_str):
    """Mengekstrak bagian variasi yang relevan untuk DAMA.ID STORE."""
    if pd.isna(var_str):
        return None
    
    var_str_clean = str(var_str).strip().upper()
    parts = [p.strip() for p in var_str_clean.split(',')]
    size_keywords = {'QPP', 'A5', 'B5', 'A6', 'A7', 'HVS', 'KORAN'}
    
    for part in parts:
        if part in size_keywords:
            return part
    
    return None

def extract_paper_and_size_variation(var_str):
    """Mengekstrak Jenis Kertas atau Ukuran/Paket dari string variasi."""
    if pd.isna(var_str):
        return ''

    var_str_clean = str(var_str).strip().upper()
    
    paper_types = {'HVS', 'QPP', 'KORAN', 'KK', 'KWARTO', 'BIGBOS', 'ART PAPER'} 
    size_package_patterns = [
        r'\b(PAKET\s*\d+)\b',
        r'\b((A|B)\d{1,2})\b'
    ]
    
    relevant_parts_found = []
    
    for paper in paper_types:
        if re.search(r'\b' + re.escape(paper) + r'\b', var_str_clean):
            relevant_parts_found.append('KORAN' if paper == 'KK' else paper)
            
    for pattern in size_package_patterns:
        matches = re.findall(pattern, var_str_clean)
        for match in matches:
             if isinstance(match, tuple):
                 relevant_parts_found.append(match[0].strip()) 
             else:
                 relevant_parts_found.append(match.strip())

    unique_parts = sorted(list(set(relevant_parts_found)))
    return ' '.join(unique_parts)

def process_rekap(order_df, income_df, seller_conv_df):
    """Fungsi untuk memproses sheet 'REKAP' (Human Store & Raka Bookstore)."""
    order_agg = order_df.groupby(['No. Pesanan', 'Nama Produk','Nama Variasi']).agg({
        'Jumlah': 'sum',
        'Harga Setelah Diskon': 'first',
        'Total Harga Produk': 'sum'
    }).reset_index()
    order_agg.rename(columns={'Jumlah': 'Jumlah Terjual'}, inplace=True)

    income_df['No. Pesanan'] = income_df['No. Pesanan'].astype(str)
    order_agg['No. Pesanan'] = order_agg['No. Pesanan'].astype(str)
    seller_conv_df['Kode Pesanan'] = seller_conv_df['Kode Pesanan'].astype(str)
    
    rekap_df = pd.merge(income_df, order_agg, on='No. Pesanan', how='left')

    if 'No. Pengajuan' not in rekap_df.columns:
        rekap_df['No. Pengajuan'] = np.nan
    rekap_df['No. Pengajuan'] = rekap_df['No. Pengajuan'].astype(str).str.strip()
    
    potential_return_orders = rekap_df[
        rekap_df['No. Pengajuan'].notna() & 
        (rekap_df['No. Pengajuan'] != 'nan') & 
        (rekap_df['No. Pengajuan'] != '')
    ]['No. Pesanan'].unique()
    
    full_return_orders = set()
    partial_return_orders = set()
    partial_return_items_map = {}
    
    for order_id in potential_return_orders:
        order_details = order_df[order_df['No. Pesanan'] == order_id]
        
        if order_details.empty:
            continue 
            
        total_items_in_order = len(order_details)
        
        returned_items = order_details[order_details['Status Pembatalan/ Pengembalian'] == 'Permintaan Disetujui']
        returned_items_count = len(returned_items)
        
        if returned_items_count == 0:
            continue 
            
        if returned_items_count > 0 and returned_items_count == total_items_in_order:
            full_return_orders.add(order_id)
        elif returned_items_count > 0 and returned_items_count < total_items_in_order:
            partial_return_orders.add(order_id)
            returned_item_keys = [
                (row['Nama Produk'], row['Nama Variasi']) 
                for _, row in returned_items.iterrows()
            ]
            partial_return_items_map[order_id] = {
                'keys': set(returned_item_keys),
                'count': returned_items_count
            }
    
    produk_khusus_raw = [
        "CUSTOM AL QURAN MENGENANG/WAFAT 40/100/1000 HARI",
        "AL QUR'AN GOLD TERMURAH",
        "Alquran Cover Emas Kertas HVS Al Aqeel Gold Murah",
        "AL-QUR'AN SAKU A7 MAHEER HAFALAN AL QUR'AN",
        "AL QUR'AN NON TERJEMAH AL AQEEL A5 KERTAS KORAN WAKAF",
        "AL QUR'AN NON TERJEMAH Al AQEEL A5 KERTAS KORAN WAKAF",
        "AL-QURAN AL AQEEL SILVER TERMURAH",
        "AL-QUR'AN TERJEMAH HC AL ALEEM A5",
        "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan",
        "AL QUR'AN A6 NON TERJEMAH HVS WARNA PASTEL",
        "Paket Wakaf Murah 50 pcs Alquran Al Aqeel | Alquran 18 Baris", 
        "Alquran Cover Emas Kertas HVS Al Aqeel A7 Gold Murah"
    ]
    
    produk_khusus = [re.sub(r'\s+', ' ', name.replace('\xa0', ' ')).strip() for name in produk_khusus_raw]

    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Nama Produk Clean Temp'] = rekap_df['Nama Produk'].astype(str).str.replace('\xa0', ' ').str.replace(r'\s+', ' ', regex=True).str.strip()
        kondisi = rekap_df['Nama Produk Clean Temp'].isin(produk_khusus)
    else:
        kondisi = pd.Series([False] * len(rekap_df), index=rekap_df.index)
    
    if 'Nama Variasi' in rekap_df.columns:
        new_product_names = rekap_df.loc[kondisi, 'Nama Produk'].copy()
    
        for idx in new_product_names.index:
            nama_produk_asli = rekap_df.loc[idx, 'Nama Produk']
            nama_produk_clean = rekap_df.loc[idx, 'Nama Produk Clean Temp']
            nama_variasi_ori = rekap_df.loc[idx, 'Nama Variasi']
    
            if pd.notna(nama_variasi_ori):
                var_str = str(nama_variasi_ori).strip()
                part_to_append = ''
    
                produk_yang_ambil_full_variasi = [
                    "CUSTOM AL QURAN MENGENANG", 
                    "AL QUR'AN GOLD TERMURAH",
                    "Alquran Cover Emas Kertas HVS Al Aqeel Gold Murah",
                    "AL-QUR'AN SAKU A7 MAHEER HAFALAN AL QUR'AN",
                    "AL-QURAN AL AQEEL SILVER TERMURAH",
                    "Paket Wakaf Murah 50 pcs Alquran Al Aqeel | Alquran 18 Baris"
                ]
                if any(produk in nama_produk_clean for produk in produk_yang_ambil_full_variasi):
                    part_to_append = var_str
                elif "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan" in nama_produk_clean:
                    if ',' in var_str:
                        part_to_append = var_str.split(',', 1)[-1].strip()
                    else:
                        part_to_append = var_str
                elif "AL-QUR'AN TERJEMAH HC AL ALEEM A5" in nama_produk_clean:
                    if 'QPP' in var_str.upper():
                        part_to_append = 'QPP'
                    elif 'HVS' in var_str.upper():
                        part_to_append = 'HVS'
                    elif 'KORAN' in var_str.upper():
                        part_to_append = 'KORAN'
                        
                elif "AL QUR'AN NON TERJEMAH Al AQEEL A5 KERTAS KORAN WAKAF" in nama_produk_clean or "AL QUR'AN A6 NON TERJEMAH HVS WARNA PASTEL" in nama_produk_clean:
                    var_upper = var_str.upper()
                    paket_match = re.search(r'(PAKET\s*ISI\s*\d+)', var_upper)
                    satuan_match = 'SATUAN' in var_upper
                
                    if paket_match:
                        part_to_append = paket_match.group(1)
                    elif satuan_match:
                        part_to_append = 'SATUAN'
                    else:
                        if ',' in var_str:
                            parts = [p.strip().upper() for p in var_str.split(',')]
                            size_keywords = {'QPP', 'A5', 'B5', 'A6', 'A7', 'HVS', 'KORAN'}
                            relevant_parts = [p for p in parts if p in size_keywords]
                            if relevant_parts:
                                part_to_append = relevant_parts[0]
                        else:
                            part_to_append = var_str

                if part_to_append:
                    new_product_names.loc[idx] = f"{nama_produk_asli} ({part_to_append})"
    
        rekap_df.loc[kondisi, 'Nama Produk'] = new_product_names
    
    if 'Nama Produk Clean Temp' in rekap_df.columns:
        rekap_df.drop(columns=['Nama Produk Clean Temp'], inplace=True)

    iklan_per_pesanan = seller_conv_df.groupby('Kode Pesanan')['Pengeluaran(Rp)'].sum().reset_index()
    rekap_df = pd.merge(rekap_df, iklan_per_pesanan, left_on='No. Pesanan', right_on='Kode Pesanan', how='left')
    rekap_df['Pengeluaran(Rp)'] = rekap_df['Pengeluaran(Rp)'].fillna(0)

    rekap_df['Total Harga Produk'] = rekap_df.get('Total Harga Produk', 0).fillna(0)
    
    product_count_per_order = rekap_df.groupby('No. Pesanan')['No. Pesanan'].transform('size')
    rekap_df['Total Penghasilan Dibagi'] = (rekap_df['Total Penghasilan'] / product_count_per_order).fillna(0)

    rekap_df['Voucher dari Penjual'] = clean_and_convert_to_numeric(rekap_df['Voucher disponsor oleh Penjual'])
    rekap_df['Promo Gratis Ongkir dari Penjual'] = clean_and_convert_to_numeric(rekap_df['Promo Gratis Ongkir dari Penjual'])

    rekap_df['Voucher dari Penjual Dibagi'] = (rekap_df['Voucher dari Penjual'] / product_count_per_order).fillna(0).abs()
    rekap_df['Gratis Ongkir dari Penjual Dibagi'] = (rekap_df['Promo Gratis Ongkir dari Penjual'] / product_count_per_order).fillna(0).abs()
    
    rekap_df['Biaya Proses Pesanan Dibagi'] = 1250 / product_count_per_order

    basis_biaya = rekap_df['Total Harga Produk'] - rekap_df['Voucher dari Penjual Dibagi']
    tahun_pesanan = pd.to_datetime(rekap_df['Waktu Pesanan Dibuat']).dt.year
    
    rekap_df['Biaya Adm 8%'] = np.where(tahun_pesanan == 2026, basis_biaya * 0.09, basis_biaya * 0.08)
    rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = basis_biaya * 0.045
    rekap_df['Biaya Layanan 2%'] = 0
    
    order_level_costs = ['Pengeluaran(Rp)', 'Total Penghasilan']
    is_first_item_mask = ~rekap_df.duplicated(subset='No. Pesanan', keep='first')
    
    for col in order_level_costs:
        if col in rekap_df.columns:
            rekap_df[col] = rekap_df[col].fillna(0)
            rekap_df[col] = rekap_df[col] * is_first_item_mask

    cost_columns_to_abs = [
        'Voucher dari Penjual', 'Pengeluaran(Rp)', 'Biaya Administrasi', 
        'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%', 
        'Biaya Proses Pesanan'
    ]
    for col in cost_columns_to_abs:
        if col in rekap_df.columns:
            rekap_df[col] = rekap_df[col].abs()

    rekap_df['Penjualan Netto'] = (
        rekap_df.get('Total Harga Produk', 0) -
        rekap_df.get('Voucher dari Penjual Dibagi', 0) -
        rekap_df.get('Pengeluaran(Rp)', 0) -
        rekap_df.get('Biaya Adm 8%', 0) -
        rekap_df.get('Biaya Layanan 2%', 0) -
        rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0) -
        rekap_df.get('Biaya Proses Pesanan Dibagi', 0) -
        rekap_df.get('Gratis Ongkir dari Penjual Dibagi', 0)
    )

    rekap_df.sort_values(by='No. Pesanan', inplace=True)
    rekap_df.reset_index(drop=True, inplace=True)

    cols_to_zero_out = [
        'Voucher dari Penjual Dibagi', 'Pengeluaran(Rp)', 'Biaya Adm 8%', 
        'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%', 
        'Biaya Proses Pesanan Dibagi', 'Gratis Ongkir dari Penjual Dibagi'
    ]
    valid_cols_to_zero = [col for col in cols_to_zero_out if col in rekap_df.columns]
    
    if full_return_orders:
        kondisi_full_retur = rekap_df['No. Pesanan'].isin(full_return_orders)
        if kondisi_full_retur.any():
            rekap_df.loc[kondisi_full_retur, valid_cols_to_zero] = 0
            rekap_df.loc[kondisi_full_retur, 'Penjualan Netto'] = rekap_df.loc[kondisi_full_retur, 'Total Penghasilan Dibagi']

    if partial_return_orders:
        if 'Jumlah Pengembalian Dana ke Pembeli' not in rekap_df.columns:
            rekap_df['Jumlah Pengembalian Dana ke Pembeli'] = 0
        
        rekap_df['Jumlah Pengembalian Dana ke Pembeli'] = 0
        
        rekap_df['__return_count__'] = rekap_df['No. Pesanan'].map(
            lambda x: partial_return_items_map.get(x, {}).get('count', 1)
        )
        
        rekap_df['Pengembalian Dana Per Item'] = (
            rekap_df['Jumlah Pengembalian Dana ke Pembeli'] / rekap_df['__return_count__']
        ).fillna(0)
        
        def is_partial_return_item(row):
            order_id = row['No. Pesanan']
            if order_id not in partial_return_items_map:
                return False
            
            item_key = (row['Nama Produk'], row['Nama Variasi'])
            return item_key in partial_return_items_map[order_id]['keys']

        kondisi_partial_item = rekap_df.apply(is_partial_return_item, axis=1)
        
        if kondisi_partial_item.any():
            rekap_df.loc[kondisi_partial_item, valid_cols_to_zero] = 0
            rekap_df.loc[kondisi_partial_item, 'Penjualan Netto'] = rekap_df.loc[kondisi_partial_item, 'Pengembalian Dana Per Item']
            
        rekap_df = rekap_df.drop(columns=['__return_count__', 'Pengembalian Dana Per Item'], errors='ignore')
    
    rekap_final = pd.DataFrame({
        'No.': np.arange(1, len(rekap_df) + 1),
        'No. Pesanan': rekap_df['No. Pesanan'],
        'Waktu Pesanan Dibuat': rekap_df['Waktu Pesanan Dibuat'],
        'Waktu Dana Dilepas': rekap_df['Tanggal Dana Dilepaskan'],
        'Nama Produk': rekap_df['Nama Produk'],
        'Jumlah Terjual': rekap_df['Jumlah Terjual'],
        'Harga Satuan': rekap_df['Harga Setelah Diskon'],
        'Total Harga Produk': rekap_df['Total Harga Produk'],
        'Voucher Ditanggung Penjual': rekap_df.get('Voucher dari Penjual Dibagi', 0),
        'Biaya Komisi AMS + PPN Shopee': rekap_df.get('Pengeluaran(Rp)', 0),
        'Biaya Adm 8%': rekap_df.get('Biaya Adm 8%', 0),
        'Biaya Layanan 2%': rekap_df.get('Biaya Layanan 2%', 0),
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0),
        'Biaya Proses Pesanan': rekap_df.get('Biaya Proses Pesanan Dibagi', 0),
        'Gratis Ongkir dari Penjual': rekap_df.get('Gratis Ongkir dari Penjual Dibagi', 0),
        'Total Penghasilan': rekap_df['Penjualan Netto'],
        'Metode Pembayaran': rekap_df.get('Metode pembayaran pembeli', '')
    })

    cols_to_blank = ['No. Pesanan', 'Waktu Pesanan Dibuat', 'Waktu Dana Dilepas']
    rekap_final.loc[rekap_final['No. Pesanan'].duplicated(), cols_to_blank] = ''

    return rekap_final.fillna(0)

def process_rekap_pacific(order_df, income_df, seller_conv_df):
    """Fungsi untuk memproses sheet 'REKAP' untuk Pacific Bookstore."""
    order_agg = order_df.groupby(['No. Pesanan', 'Nama Produk' ,'Nama Variasi']).agg({
        'Jumlah': 'sum',
        'Harga Setelah Diskon': 'first',
        'Total Harga Produk': 'sum'
    }).reset_index()
    order_agg.rename(columns={'Jumlah': 'Jumlah Terjual'}, inplace=True)

    income_df['No. Pesanan'] = income_df['No. Pesanan'].astype(str)
    order_agg['No. Pesanan'] = order_agg['No. Pesanan'].astype(str)
    seller_conv_df['Kode Pesanan'] = seller_conv_df['Kode Pesanan'].astype(str)
    
    rekap_df = pd.merge(income_df, order_agg, on='No. Pesanan', how='left')

    if 'No. Pengajuan' not in rekap_df.columns:
        rekap_df['No. Pengajuan'] = np.nan
    rekap_df['No. Pengajuan'] = rekap_df['No. Pengajuan'].astype(str).str.strip()
    
    potential_return_orders = rekap_df[
        rekap_df['No. Pengajuan'].notna() & 
        (rekap_df['No. Pengajuan'] != 'nan') & 
        (rekap_df['No. Pengajuan'] != '')
    ]['No. Pesanan'].unique()
    
    full_return_orders = set()
    partial_return_orders = set()
    partial_return_items_map = {}
    
    for order_id in potential_return_orders:
        order_details = order_df[order_df['No. Pesanan'] == order_id]
        
        if order_details.empty:
            continue 
            
        total_items_in_order = len(order_details)
        
        returned_items = order_details[order_details['Status Pembatalan/ Pengembalian'] == 'Permintaan Disetujui']
        returned_items_count = len(returned_items)
        
        if returned_items_count == 0:
            continue 
            
        if returned_items_count > 0 and returned_items_count == total_items_in_order:
            full_return_orders.add(order_id)
        elif returned_items_count > 0 and returned_items_count < total_items_in_order:
            partial_return_orders.add(order_id)
            returned_item_keys = [
                (row['Nama Produk'], row['Nama Variasi']) 
                for _, row in returned_items.iterrows()
            ]
            partial_return_items_map[order_id] = {
                'keys': set(returned_item_keys),
                'count': returned_items_count
            }
    
    produk_khusus_raw = [
        "CUSTOM AL QURAN MENGENANG/WAFAT 40/100/1000 HARI",
        "AL QUR'AN GOLD TERMURAH",
        "Alquran Cover Emas Kertas HVS Al Aqeel Gold Murah",
        "TERBARU Al Quran Edisi Tahlilan Pengganti Buku Yasin Al Aqeel A6 Kertas HVS | SURABAYA | Mushaf Untuk Pengajian Kado Islami Hampers",
        "Al Quran Terjemah Al Aleem A5 HVS 15 Baris | SURABAYA | Alquran Untuk Pengajian Majelis Taklim",
        "Al Quran Saku Resleting Al Quddus A7 QPP Cover Kulit | SURABAYA | Untuk Santri Traveler Muslim",
        "Al Quran Wakaf Ibtida Al Quddus A5 Kertas HVS | Alquran SURABAYA",
        "Al Fikrah Al Quran Terjemah Fitur Lengkap A5 Kertas HVS | Alquran SURABAYA",
        "Al Quddus Al Quran Wakaf Ibtida A5 Kertas HVS | Alquran SURABAYA",
        "Al Quran Terjemah Al Aleem A5 Kertas HVS 15 Baris | SURABAYA | Alquran Untuk Majelis Taklim Kajian",
        "Al Quran Terjemah Per Kata A5 | Tajwid 2 Warna | Alquran Al Fikrah HVS 15 Baris | SURABAYA",
        "Al Quran Saku Resleting Al Quddus A7 Cover Kulit Kertas QPP | Alquran SURABAYA",
        "Al Quran Saku Pastel Al Aqeel A6 Kertas HVS | SURABAYA | Alquran Untuk Wakaf Hadiah Islami Hampers",
        "Al Quran Untuk Wakaf Al Aqeel A5 Kertas Koran 18 Baris | SURABAYA | Alquran Hadiah Islami Hampers",
        "Al Qur'an Untuk Wakaf Al Aqeel A5 Kertas Koran 18 Baris",
        "Alquran Edisi Tahlilan Lebih Mulia Daripada Buku Yasin Biasa | Al Aqeel A6 Kertas HVS | SURABAYA |",
        "PAKET MURAH ALQURAN AL AQEEL MUSHAF NON TERJEMAHAN | SURABAYA | al quran Wakaf/Shodaqoh hadiah hampers islami",
        "Alquran GOLD Hard Cover Al Aqeel Kertas HVS | SURABAYA | Alquran untuk Pengajian Wakaf Hadiah Islami Hampers"
    ]
    
    produk_khusus = [re.sub(r'\s+', ' ', name.replace('\xa0', ' ')).strip() for name in produk_khusus_raw]

    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Nama Produk Clean Temp'] = rekap_df['Nama Produk'].astype(str).str.replace('\xa0', ' ').str.replace(r'\s+', ' ', regex=True).str.strip()
        kondisi = rekap_df['Nama Produk Clean Temp'].isin(produk_khusus)
    else:
        kondisi = pd.Series([False] * len(rekap_df), index=rekap_df.index)
    
    if 'Nama Variasi' in rekap_df.columns:
        new_product_names = rekap_df.loc[kondisi, 'Nama Produk'].copy()
    
        for idx in new_product_names.index:
            nama_produk_asli = rekap_df.loc[idx, 'Nama Produk']
            nama_produk_clean = rekap_df.loc[idx, 'Nama Produk Clean Temp']
            nama_variasi_ori = rekap_df.loc[idx, 'Nama Variasi']
    
            if pd.notna(nama_variasi_ori):
                var_str = str(nama_variasi_ori).strip()
                part_to_append = ''
                
                val_raw = rekap_df.loc[idx, 'Harga Setelah Diskon']
                
                try:
                    harga_satuan = int(float(str(val_raw).replace('.', '').replace(',', '')))
                except:
                    harga_satuan = 0
    
                produk_yang_ambil_full_variasi = [
                    "CUSTOM AL QURAN MENGENANG", 
                    "AL QUR'AN GOLD TERMURAH",
                    "Alquran Cover Emas Kertas HVS Al Aqeel Gold Murah",
                    "AL-QUR'AN SAKU A7 MAHEER HAFALAN AL QUR'AN",
                    "Alquran GOLD Hard Cover Al Aqeel Kertas HVS | SURABAYA | Alquran untuk Pengajian Wakaf Hadiah Islami Hampers",
                    "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan"
                ]
                if any(produk in nama_produk_clean for produk in produk_yang_ambil_full_variasi):
                    part_to_append = var_str
                elif "PAKET MURAH ALQURAN AL AQEEL MUSHAF NON TERJEMAHAN | SURABAYA | al quran Wakaf/Shodaqoh hadiah hampers islami" in nama_produk_clean:
                    part_to_append = re.sub(r'\(.*?\)', '', var_str).strip()
                elif "Alquran Edisi Tahlilan Lebih Mulia Daripada Buku Yasin Biasa | Al Aqeel A6 Kertas HVS | SURABAYA |" in nama_produk_clean:
                    if ',' in var_str:
                        spesifikasi = var_str.split(',', 1)[-1].strip()
                        part_to_append = spesifikasi
                    else:
                        warna_keywords = ['MERAH', 'COKLAT', 'BIRU', 'UNGU', 'HIJAU', 'RANDOM', 'HITAM']
                        is_warna = any(w in var_str.upper() for w in warna_keywords)
                        
                        if not is_warna:
                            part_to_append = var_str
                        else:
                            part_to_append = ''
                
                elif "Al Quran Saku Pastel Al Aqeel A6 Kertas HVS | SURABAYA | Alquran Untuk Wakaf Hadiah Islami Hampers" in nama_produk_clean:
                    if harga_satuan == 19500:
                        part_to_append = "GROSIR 1-2"
                    elif harga_satuan == 19200:
                        part_to_append = "GROSIR 3-4"
                    elif harga_satuan == 18900:
                        part_to_append = "GROSIR 5-6"
                    elif harga_satuan == 18600:
                        part_to_append = "GROSIR > 7"
                
                elif "Al Quran Untuk Wakaf Al Aqeel A5 Kertas Koran 18 Baris | SURABAYA | Alquran Hadiah Islami Hampers" in nama_produk_clean:
                    if harga_satuan == 21800:
                        part_to_append = "GROSIR 1-2"
                    elif harga_satuan == 21550:
                        part_to_append = "GROSIR 3-4"
                    elif harga_satuan == 21300:
                        part_to_append = "GROSIR 5-6"
                    elif harga_satuan == 21000:
                        part_to_append = "GROSIR > 7"
    
            
                elif "Al Qur'an Untuk Wakaf Al Aqeel A5 Kertas Koran 18 Baris" in nama_produk_clean:
                    var_upper = var_str.upper()
                    paket_match = re.search(r'(PAKET\s*ISI\s*\d+)', var_upper)
                    satuan_match = 'SATUAN' in var_upper
                    
                    if paket_match:
                        part_to_append = paket_match.group(1)
                    elif satuan_match:
                        part_to_append = 'SATUAN'
                    else:
                        if ',' in var_str:
                            parts = [p.strip().upper() for p in var_str.split(',')]
                            size_keywords = {'QPP', 'A5', 'B5', 'A6', 'A7', 'HVS', 'KORAN'}
                            relevant_parts = [p for p in parts if p in size_keywords]
                            if relevant_parts:
                                part_to_append = relevant_parts[0]
                        else:
                            part_to_append = var_str

                if part_to_append:
                    new_product_names.loc[idx] = f"{nama_produk_asli} ({part_to_append})"
    
        rekap_df.loc[kondisi, 'Nama Produk'] = new_product_names
    
    if 'Nama Produk Clean Temp' in rekap_df.columns:
        rekap_df.drop(columns=['Nama Produk Clean Temp'], inplace=True)

    iklan_per_pesanan = seller_conv_df.groupby('Kode Pesanan')['Pengeluaran(Rp)'].sum().reset_index()
    rekap_df = pd.merge(rekap_df, iklan_per_pesanan, left_on='No. Pesanan', right_on='Kode Pesanan', how='left')
    rekap_df['Pengeluaran(Rp)'] = rekap_df['Pengeluaran(Rp)'].fillna(0)

    rekap_df['Total Harga Produk'] = rekap_df.get('Total Harga Produk', 0).fillna(0)
    
    product_count_per_order = rekap_df.groupby('No. Pesanan')['No. Pesanan'].transform('size')
    rekap_df['Total Penghasilan Dibagi'] = (rekap_df['Total Penghasilan'] / product_count_per_order).fillna(0)

    rekap_df['Voucher dari Penjual'] = clean_and_convert_to_numeric(rekap_df['Voucher disponsor oleh Penjual'])
    rekap_df['Promo Gratis Ongkir dari Penjual'] = clean_and_convert_to_numeric(rekap_df['Promo Gratis Ongkir dari Penjual'])

    rekap_df['Voucher dari Penjual Dibagi'] = (rekap_df['Voucher dari Penjual'] / product_count_per_order).fillna(0).abs()
    rekap_df['Gratis Ongkir dari Penjual Dibagi'] = (rekap_df['Promo Gratis Ongkir dari Penjual'] / product_count_per_order).fillna(0).abs()
    
    rekap_df['Biaya Proses Pesanan Dibagi'] = 1250 / product_count_per_order

    basis_biaya = rekap_df['Total Harga Produk'] - rekap_df['Voucher dari Penjual Dibagi']
    tahun_pesanan = pd.to_datetime(rekap_df['Waktu Pesanan Dibuat']).dt.year
    
    rekap_df['Biaya Adm 8%'] = np.where(tahun_pesanan == 2026, basis_biaya * 0.09, basis_biaya * 0.08)
    
    rekap_df['Biaya Layanan_Clean'] = clean_and_convert_to_numeric(rekap_df.get('Biaya Layanan', 0))
    rekap_df['Biaya Layanan 4,5%'] = (rekap_df['Biaya Layanan_Clean'] / product_count_per_order).fillna(0).abs()
    rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = 0
    
    order_level_costs = ['Pengeluaran(Rp)', 'Total Penghasilan']
    is_first_item_mask = ~rekap_df.duplicated(subset='No. Pesanan', keep='first')
    
    for col in order_level_costs:
        if col in rekap_df.columns:
            rekap_df[col] = rekap_df[col].fillna(0)
            rekap_df[col] = rekap_df[col] * is_first_item_mask

    cost_columns_to_abs = [
        'Voucher dari Penjual', 'Pengeluaran(Rp)', 'Biaya Administrasi', 
        'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%', 
        'Biaya Proses Pesanan'
    ]
    for col in cost_columns_to_abs:
        if col in rekap_df.columns:
            rekap_df[col] = rekap_df[col].abs()

    rekap_df['Penjualan Netto'] = (
        rekap_df.get('Total Harga Produk', 0) -
        rekap_df.get('Voucher dari Penjual Dibagi', 0) -
        rekap_df.get('Pengeluaran(Rp)', 0) -
        rekap_df.get('Biaya Adm 8%', 0) -
        rekap_df.get('Biaya Layanan 2%', 0) -
        rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0) -
        rekap_df.get('Biaya Proses Pesanan Dibagi', 0) -
        rekap_df.get('Gratis Ongkir dari Penjual Dibagi', 0)
    )

    rekap_df.sort_values(by='No. Pesanan', inplace=True)
    rekap_df.reset_index(drop=True, inplace=True)

    cols_to_zero_out = [
        'Voucher dari Penjual Dibagi', 'Pengeluaran(Rp)', 'Biaya Adm 8%', 
        'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%', 
        'Biaya Proses Pesanan Dibagi', 'Gratis Ongkir dari Penjual Dibagi'
    ]
    valid_cols_to_zero = [col for col in cols_to_zero_out if col in rekap_df.columns]
    
    if full_return_orders:
        kondisi_full_retur = rekap_df['No. Pesanan'].isin(full_return_orders)
        if kondisi_full_retur.any():
            rekap_df.loc[kondisi_full_retur, valid_cols_to_zero] = 0
            rekap_df.loc[kondisi_full_retur, 'Penjualan Netto'] = rekap_df.loc[kondisi_full_retur, 'Total Penghasilan Dibagi']

    if partial_return_orders:
        if 'Jumlah Pengembalian Dana ke Pembeli' not in rekap_df.columns:
            rekap_df['Jumlah Pengembalian Dana ke Pembeli'] = 0
        
        rekap_df['Jumlah Pengembalian Dana ke Pembeli'] = 0
        
        rekap_df['__return_count__'] = rekap_df['No. Pesanan'].map(
            lambda x: partial_return_items_map.get(x, {}).get('count', 1)
        )
        
        rekap_df['Pengembalian Dana Per Item'] = (
            rekap_df['Jumlah Pengembalian Dana ke Pembeli'] / rekap_df['__return_count__']
        ).fillna(0)
        
        def is_partial_return_item(row):
            order_id = row['No. Pesanan']
            if order_id not in partial_return_items_map:
                return False
            
            item_key = (row['Nama Produk'], row['Nama Variasi'])
            return item_key in partial_return_items_map[order_id]['keys']

        kondisi_partial_item = rekap_df.apply(is_partial_return_item, axis=1)
        
        if kondisi_partial_item.any():
            rekap_df.loc[kondisi_partial_item, valid_cols_to_zero] = 0
            rekap_df.loc[kondisi_partial_item, 'Penjualan Netto'] = rekap_df.loc[kondisi_partial_item, 'Pengembalian Dana Per Item']
            
        rekap_df = rekap_df.drop(columns=['__return_count__', 'Pengembalian Dana Per Item'], errors='ignore')
    
    rekap_final = pd.DataFrame({
        'No.': np.arange(1, len(rekap_df) + 1),
        'No. Pesanan': rekap_df['No. Pesanan'],
        'Waktu Pesanan Dibuat': rekap_df['Waktu Pesanan Dibuat'],
        'Waktu Dana Dilepas': rekap_df['Tanggal Dana Dilepaskan'],
        'Nama Produk': rekap_df['Nama Produk'],
        'Jumlah Terjual': rekap_df['Jumlah Terjual'],
        'Harga Satuan': rekap_df['Harga Setelah Diskon'],
        'Total Harga Produk': rekap_df['Total Harga Produk'],
        'Voucher Ditanggung Penjual': rekap_df.get('Voucher dari Penjual Dibagi', 0),
        'Biaya Komisi AMS + PPN Shopee': rekap_df.get('Pengeluaran(Rp)', 0),
        'Biaya Adm 8%': rekap_df.get('Biaya Adm 8%', 0),
        'Biaya Layanan 4,5%': rekap_df.get('Biaya Layanan 4,5%', 0),
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0),
        'Biaya Proses Pesanan': rekap_df.get('Biaya Proses Pesanan Dibagi', 0),
        'Gratis Ongkir dari Penjual': rekap_df.get('Gratis Ongkir dari Penjual Dibagi', 0),
        'Total Penghasilan': rekap_df['Penjualan Netto'],
        'Metode Pembayaran': rekap_df.get('Metode pembayaran pembeli', '')
    })

    cols_to_blank = ['No. Pesanan', 'Waktu Pesanan Dibuat', 'Waktu Dana Dilepas']
    rekap_final.loc[rekap_final['No. Pesanan'].duplicated(), cols_to_blank] = ''

    return rekap_final.fillna(0)

def process_rekap_dama(order_df, income_df, seller_conv_df):
    """Fungsi untuk memproses sheet 'REKAP' untuk DAMA.ID STORE."""
    if 'Nama Variasi' in order_df.columns:
        order_df['Nama Variasi'] = order_df['Nama Variasi'].fillna('')
    else:
        order_df['Nama Variasi'] = ''
        
    order_agg = order_df.groupby(['No. Pesanan', 'Nama Produk', 'Nama Variasi']).agg({
        'Jumlah': 'sum',
        'Harga Setelah Diskon': 'first',
        'Total Harga Produk': 'sum'
    }).reset_index()
    order_agg.rename(columns={'Jumlah': 'Jumlah Terjual'}, inplace=True)

    income_df['No. Pesanan'] = income_df['No. Pesanan'].astype(str)
    order_agg['No. Pesanan'] = order_agg['No. Pesanan'].astype(str)
    
    rekap_df = pd.merge(income_df, order_agg, on='No. Pesanan', how='left')

    mask_produk = (
        rekap_df['Nama Produk'] ==
        'Paket Hemat Paket Grosir Al Quran | AQ Al Aqeel Wakaf Kerta koran Non Terjemah'
    )

    rekap_df.loc[mask_produk & (rekap_df['Harga Setelah Diskon'] == 21799), 'Nama Variasi'] = 'GROSIR 1-2'
    rekap_df.loc[mask_produk & (rekap_df['Harga Setelah Diskon'] == 21499), 'Nama Variasi'] = 'GROSIR 3-4'
    rekap_df.loc[mask_produk & (rekap_df['Harga Setelah Diskon'] == 21229), 'Nama Variasi'] = 'GROSIR 5-6'
    rekap_df.loc[mask_produk & (rekap_df['Harga Setelah Diskon'] == 21099), 'Nama Variasi'] = 'GROSIR >7'

    if 'No. Pengajuan' not in rekap_df.columns:
        rekap_df['No. Pengajuan'] = np.nan
    rekap_df['No. Pengajuan'] = rekap_df['No. Pengajuan'].astype(str).str.strip()
    
    potential_return_orders = rekap_df[
        rekap_df['No. Pengajuan'].notna() & 
        (rekap_df['No. Pengajuan'] != 'nan') & 
        (rekap_df['No. Pengajuan'] != '')
    ]['No. Pesanan'].unique()
    
    full_return_orders = set()
    partial_return_orders = set()
    partial_return_items_map = {}
    
    for order_id in potential_return_orders:
        order_details = order_df[order_df['No. Pesanan'] == order_id]
        
        if order_details.empty:
            continue 
            
        total_items_in_order = len(order_details)
        
        returned_items = order_details[order_details['Status Pembatalan/ Pengembalian'] == 'Permintaan Disetujui']
        returned_items_count = len(returned_items)
        
        if returned_items_count == 0:
            continue 
            
        if returned_items_count > 0 and returned_items_count == total_items_in_order:
            full_return_orders.add(order_id)
        elif returned_items_count > 0 and returned_items_count < total_items_in_order:
            partial_return_orders.add(order_id)
            returned_item_keys = [
                (row['Nama Produk'], row['Nama Variasi']) 
                for _, row in returned_items.iterrows()
            ]
            partial_return_items_map[order_id] = {
                'keys': set(returned_item_keys),
                'count': returned_items_count
            }
    
    if not seller_conv_df.empty:
        seller_conv_df['Kode Pesanan'] = seller_conv_df['Kode Pesanan'].astype(str)
        iklan_per_pesanan = seller_conv_df.groupby('Kode Pesanan')['Pengeluaran(Rp)'].sum().reset_index()
        rekap_df = pd.merge(rekap_df, iklan_per_pesanan, left_on='No. Pesanan', right_on='Kode Pesanan', how='left')
        rekap_df['Pengeluaran(Rp)'] = rekap_df['Pengeluaran(Rp)'].fillna(0)
    else:
        rekap_df['Pengeluaran(Rp)'] = 0

    rekap_df['Total Harga Produk'] = rekap_df.get('Total Harga Produk', 0).fillna(0) 
    
    product_count_per_order = rekap_df.groupby('No. Pesanan')['No. Pesanan'].transform('size')
    rekap_df['Total Penghasilan Dibagi'] = (rekap_df['Total Penghasilan'] / product_count_per_order).fillna(0)

    rekap_df['Voucher dari Penjual'] = clean_and_convert_to_numeric(rekap_df['Voucher disponsor oleh Penjual'])
    rekap_df['Promo Gratis Ongkir dari Penjual'] = clean_and_convert_to_numeric(rekap_df['Promo Gratis Ongkir dari Penjual'])

    rekap_df['Voucher dari Penjual Dibagi'] = (rekap_df['Voucher dari Penjual'] / product_count_per_order).fillna(0).abs()
    rekap_df['Gratis Ongkir dari Penjual Dibagi'] = (rekap_df['Promo Gratis Ongkir dari Penjual'] / product_count_per_order).fillna(0).abs()
    
    rekap_df['Biaya Proses Pesanan Dibagi'] = 1250 / product_count_per_order

    rekap_df['Biaya Layanan 2%'] = 0

    basis_biaya = rekap_df['Total Harga Produk'] - rekap_df['Voucher dari Penjual Dibagi']
    tahun_pesanan = pd.to_datetime(rekap_df['Waktu Pesanan Dibuat']).dt.year
    
    rekap_df['Biaya Adm 8%'] = np.where(tahun_pesanan == 2026, basis_biaya * 0.09, basis_biaya * 0.08)
    rekap_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] = basis_biaya * 0.045
    
    order_level_costs = ['Pengeluaran(Rp)', 'Total Penghasilan']
    is_first_item_mask = ~rekap_df.duplicated(subset='No. Pesanan', keep='first')
    
    for col in order_level_costs:
        if col in rekap_df.columns:
            rekap_df[col] = rekap_df[col].fillna(0)
            rekap_df[col] = rekap_df[col] * is_first_item_mask

    cost_columns_to_abs = [
        'Voucher dari Penjual', 'Pengeluaran(Rp)', 'Biaya Adm 8%', 
        'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%', 
    ]
    for col in cost_columns_to_abs:
        if col in rekap_df.columns:
             if pd.api.types.is_numeric_dtype(rekap_df[col]):
                  rekap_df[col] = rekap_df[col].abs()

    rekap_df['Penjualan Netto'] = (
        rekap_df.get('Total Harga Produk', 0) -
        rekap_df.get('Voucher dari Penjual Dibagi', 0) -
        rekap_df.get('Pengeluaran(Rp)', 0) -
        rekap_df.get('Biaya Adm 8%', 0) -
        rekap_df.get('Biaya Layanan 2%', 0) -
        rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0) -
        rekap_df.get('Biaya Proses Pesanan Dibagi', 0) -
        rekap_df.get('Gratis Ongkir dari Penjual Dibagi', 0)
    )

    rekap_df.sort_values(by='No. Pesanan', inplace=True)
    rekap_df.reset_index(drop=True, inplace=True)

    cols_to_zero_out = [
        'Voucher dari Penjual Dibagi', 'Pengeluaran(Rp)', 'Biaya Adm 8%', 
        'Biaya Layanan 2%', 'Biaya Layanan Gratis Ongkir Xtra 4,5%', 
        'Biaya Proses Pesanan Dibagi', 'Gratis Ongkir dari Penjual Dibagi'
    ]
    valid_cols_to_zero = [col for col in cols_to_zero_out if col in rekap_df.columns]
    
    if full_return_orders:
        kondisi_full_retur = rekap_df['No. Pesanan'].isin(full_return_orders)
        if kondisi_full_retur.any():
            rekap_df.loc[kondisi_full_retur, valid_cols_to_zero] = 0
            rekap_df.loc[kondisi_full_retur, 'Penjualan Netto'] = rekap_df.loc[kondisi_full_retur, 'Total Penghasilan Dibagi']

    if partial_return_orders:
        if 'Jumlah Pengembalian Dana ke Pembeli' not in rekap_df.columns:
            rekap_df['Jumlah Pengembalian Dana ke Pembeli'] = 0
        
        rekap_df['Jumlah Pengembalian Dana ke Pembeli'] = 0
        
        rekap_df['__return_count__'] = rekap_df['No. Pesanan'].map(
            lambda x: partial_return_items_map.get(x, {}).get('count', 1)
        )
        
        rekap_df['Pengembalian Dana Per Item'] = (
            rekap_df['Jumlah Pengembalian Dana ke Pembeli'] / rekap_df['__return_count__']
        ).fillna(0)
        
        def is_partial_return_item(row):
            order_id = row['No. Pesanan']
            if order_id not in partial_return_items_map:
                return False
            
            item_key = (row['Nama Produk'], row['Nama Variasi'])
            return item_key in partial_return_items_map[order_id]['keys']

        kondisi_partial_item = rekap_df.apply(is_partial_return_item, axis=1)
        
        if kondisi_partial_item.any():
            rekap_df.loc[kondisi_partial_item, valid_cols_to_zero] = 0
            rekap_df.loc[kondisi_partial_item, 'Penjualan Netto'] = rekap_df.loc[kondisi_partial_item, 'Pengembalian Dana Per Item']
            
        rekap_df = rekap_df.drop(columns=['__return_count__', 'Pengembalian Dana Per Item'], errors='ignore')
    
    rekap_final = pd.DataFrame({
        'No.': np.arange(1, len(rekap_df) + 1),
        'No. Pesanan': rekap_df['No. Pesanan'],
        'Waktu Pesanan Dibuat': rekap_df['Waktu Pesanan Dibuat'],
        'Waktu Dana Dilepas': rekap_df['Tanggal Dana Dilepaskan'],
        'Nama Produk': rekap_df['Nama Produk'],
        'Nama Variasi': rekap_df['Nama Variasi'],
        'Jumlah Terjual': rekap_df['Jumlah Terjual'],
        'Harga Satuan': rekap_df['Harga Setelah Diskon'],
        'Total Harga Produk': rekap_df['Total Harga Produk'],
        'Voucher Ditanggung Penjual': rekap_df.get('Voucher dari Penjual Dibagi', 0),
        'Biaya Komisi AMS + PPN Shopee': rekap_df.get('Pengeluaran(Rp)', 0),
        'Biaya Adm 8%': rekap_df.get('Biaya Adm 8%', 0),
        'Biaya Layanan 2%': rekap_df.get('Biaya Layanan 2%', 0),
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': rekap_df.get('Biaya Layanan Gratis Ongkir Xtra 4,5%', 0),
        'Biaya Proses Pesanan': rekap_df.get('Biaya Proses Pesanan Dibagi', 0),
        'Gratis Ongkir dari Penjual': rekap_df.get('Gratis Ongkir dari Penjual Dibagi', 0),
        'Total Penghasilan': rekap_df['Penjualan Netto'],
        'Metode Pembayaran': rekap_df.get('Metode pembayaran pembeli', '')
    })

    cols_to_blank = ['No. Pesanan', 'Waktu Pesanan Dibuat', 'Waktu Dana Dilepas']
    rekap_final.loc[rekap_final['No. Pesanan'].duplicated(), cols_to_blank] = ''

    return rekap_final.fillna(0)

def process_iklan(iklan_df):
    """Fungsi untuk memproses dan membuat sheet 'IKLAN'."""
    iklan_df['Nama Iklan Clean'] = iklan_df['Nama Iklan'].str.replace(r'\s*baris\s*\[\d+\]$', '', regex=True).str.strip()
    iklan_df['Nama Iklan Clean'] = iklan_df['Nama Iklan Clean'].str.replace(r'\s*\[\d+\]$', '', regex=True).str.strip()
    
    iklan_agg = iklan_df.groupby('Nama Iklan Clean').agg({
        'Dilihat': 'sum',
        'Jumlah Klik': 'sum',
        'Biaya': 'sum',
        'Produk Terjual': 'sum',
        'Omzet Penjualan': 'sum'
    }).reset_index()
    iklan_agg.rename(columns={'Nama Iklan Clean': 'Nama Iklan'}, inplace=True)

    total_row = pd.DataFrame({
        'Nama Iklan': ['TOTAL'],
        'Dilihat': [iklan_agg['Dilihat'].sum()],
        'Jumlah Klik': [iklan_agg['Jumlah Klik'].sum()],
        'Biaya': [iklan_agg['Biaya'].sum()],
        'Produk Terjual': [iklan_agg['Produk Terjual'].sum()],
        'Omzet Penjualan': [iklan_agg['Omzet Penjualan'].sum()]
    })
    
    iklan_final = pd.concat([iklan_agg, total_row], ignore_index=True)
    return iklan_final

def get_harga_beli_fuzzy(nama_produk, katalog_df, score_threshold_primary=80, score_threshold_fallback=75):
    """Mencari harga beli dari katalog."""
    try:
        search_name = str(nama_produk).strip()
        if not search_name:
            return 0

        s = search_name.upper()
        s_clean = re.sub(r'[^A-Z0-9\s×xX\-]', ' ', s)
        s_clean = re.sub(r'\s+', ' ', s_clean).strip()

        ukuran_found = None
        ukuran_patterns = [
            r'\bA[0-9]\b', r'\bB[0-9]\b', r'\b\d{1,3}\s*[x×X]\s*\d{1,3}\b', r'\b\d{1,3}\s*CM\b'
        ]
        for pat in ukuran_patterns:
            m = re.search(pat, s_clean)
            if m:
                ukuran_found = m.group(0).replace(' ', '').upper()
                break

        jenis_kertas_map = {
            'HVS': 'HVS', 'QPP': 'QPP', 'KORAN': 'KORAN', 'KK': 'KORAN',
            'GLOSSY':'GLOSSY','DUPLEX':'DUPLEX','ART':'ART','COVER':'COVER',
            'MATT':'MATT','MATTE':'MATTE','CTP':'CTP','BOOK PAPER':'BOOK PAPER',
            'ART PAPER': 'ART PAPER', 'ART PAPER': 'Art Paper'
        }
        jenis_kertas_tokens_to_search = list(jenis_kertas_map.keys())
        
        jenis_found = None
        s_clean_words = set(s_clean.split())
        
        for token_to_find in jenis_kertas_tokens_to_search:
            if token_to_find in s_clean_words:
                jenis_found = jenis_kertas_map[token_to_find]
                break

        candidates = katalog_df.copy()
        if ukuran_found:
            candidates = candidates[candidates['UKURAN_NORM'].str.contains(re.escape(ukuran_found), na=False)]
        if jenis_found and not candidates.empty:
            candidates = candidates[candidates['JENIS_KERTAS_NORM'].str.contains(jenis_found, na=False)]

        if candidates.empty:
            candidates = katalog_df.copy()

        best_score, best_price, best_title = 0, 0, ""
        for _, row in candidates.iterrows():
            title = str(row['JUDUL_NORM'])
            score = fuzz.token_set_ratio(s_clean, title)
            if score > best_score or (score == best_score and len(title) > len(best_title)):
                best_score, best_price, best_title = score, row.get('KATALOG_HARGA_NUM', 0), title

        if best_score >= score_threshold_primary and best_price > 0:
            return float(best_price)

        best_score2, best_price2 = best_score, best_price
        for _, row in katalog_df.iterrows():
            title = str(row['JUDUL_NORM'])
            score = fuzz.token_set_ratio(s_clean, title)
            if score > best_score2 or (score == best_score2 and len(title) > len(best_title)):
                best_score2, best_price2, best_title = score, row.get('KATALOG_HARGA_NUM', 0), title

        if best_score2 >= score_threshold_fallback and best_price2 > 0:
            return float(best_price2)

        return 0
    except Exception:
        return 0

def calculate_eksemplar(nama_produk, jumlah_terjual):
    """Menghitung jumlah eksemplar berdasarkan 'PAKET ISI X' atau 'SATUAN'."""
    try:
        nama_produk_upper = str(nama_produk).upper()
        
        paket_match = re.search(r'PAKET\s*ISI\s*(\d+)', nama_produk_upper)
        satuan_match = 'SATUAN' in nama_produk_upper
        paket_khusus = re.search(r'PAKET WAKAF MURAH 50 PCS', nama_produk_upper)
        
        faktor = 1
        
        if paket_match:
            faktor = int(paket_match.group(1))
        elif satuan_match:
            faktor = 1
        elif paket_khusus:
            faktor = 50
            
        return jumlah_terjual * faktor
    except Exception:
        return jumlah_terjual

def get_eksemplar_multiplier(nama_produk):
    if pd.isna(nama_produk): return 1
    nama_produk = str(nama_produk).upper()
        
    match = re.search(r'(?:PAKET|ISI)\s*(?:ISI\s*)?(\d+)', nama_produk)
    if match:
        return int(match.group(1))
    
    if 'SATUAN' in nama_produk:
        return 1
    return 1
    
def process_summary(rekap_df, iklan_final_df, katalog_df, harga_custom_tlj_df, store_type):
    """Fungsi untuk memproses sheet 'SUMMARY'."""
    rekap_copy = rekap_df.copy()
    rekap_copy['No. Pesanan'] = rekap_copy['No. Pesanan'].replace('', np.nan).ffill()

    kondisi_retur_summary = rekap_copy['Total Penghasilan'] <= 0
    
    rekap_copy.loc[kondisi_retur_summary, 'Jumlah Terjual'] = 0
    rekap_copy.loc[kondisi_retur_summary, 'Total Harga Produk'] = 0

    biaya_layanan_col = 'Biaya Layanan 4,5%' if store_type == 'Pacific Bookstore' else 'Biaya Layanan 2%'
    summary_df = rekap_copy.groupby(['Nama Produk', 'Harga Satuan'], as_index=False).agg({
        'Jumlah Terjual': 'sum', 
        'Total Harga Produk': 'sum',
        'Voucher Ditanggung Penjual': 'sum', 'Biaya Komisi AMS + PPN Shopee': 'sum',
        'Biaya Adm 8%': 'sum', biaya_layanan_col: 'sum',
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': 'sum', 'Biaya Proses Pesanan': 'sum',
        'Total Penghasilan': 'sum'
    })

    summary_df = summary_df[summary_df['Total Penghasilan'] != 0].copy()

    summary_df['Iklan Klik'] = 0.0
    
    produk_khusus = [
        "CUSTOM AL QURAN MENGENANG/WAFAT 40/100/1000 HARI",
        "AL QUR'AN GOLD TERMURAH",
        "AL QUR'AN A6 NON TERJEMAH HVS WARNA PASTEL",
        "Alquran Cover Emas Kertas HVS Al Aqeel Gold Murah",
        "Alquran Cover Emas Kertas HVS Al Aqeel A5 Gold Murah",
        "Alquran Cover Emas Kertas HVS Al Aqeel A7 Gold Murah", 
        "Al Qur'an Untuk Wakaf Al Aqeel A5 Kertas Koran 18 Baris",
        "AL-QUR'AN SAKU A7 MAHEER HAFALAN AL QUR'AN",
        "AL-QUR'AN TERJEMAH HC AL ALEEM A5",
        "AL-QURAN AL AQEEL SILVER TERMURAH",
        "AL QUR'AN NON TERJEMAH Al AQEEL A5 KERTAS KORAN WAKAF",
        "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan",
        "TERBARU Al Quran Edisi Tahlilan Pengganti Buku Yasin Al Aqeel A6 Kertas HVS | SURABAYA | Mushaf Untuk Pengajian Kado Islami Hampers",
        "Al Quran Terjemah Al Aleem A5 HVS 15 Baris | SURABAYA | Alquran Untuk Pengajian Majelis Taklim",
        "Al Quran Saku Resleting Al Quddus A7 QPP Cover Kulit | SURABAYA | Untuk Santri Traveler Muslim",
        "Al Quran Wakaf Ibtida Al Quddus A5 Kertas HVS | Alquran SURABAYA",
        "Al Fikrah Al Quran Terjemah Fitur Lengkap A5 Kertas HVS | Alquran SURABAYA",
        "Al Quddus Al Quran Wakaf Ibtida A5 Kertas HVS | Alquran SURABAYA",
        "Al Quran Terjemah Al Aleem A5 Kertas HVS 15 Baris | SURABAYA | Alquran Untuk Majelis Taklim Kajian",
        "Al Quran Terjemah Per Kata A5 | Tajwid 2 Warna | Alquran Al Fikrah HVS 15 Baris | SURABAYA",
        "Al Quran Saku Resleting Al Quddus A7 Cover Kulit Kertas QPP | Alquran SURABAYA",
        "Al Quran Saku Pastel Al Aqeel A6 Kertas HVS | SURABAYA | Alquran Untuk Wakaf Hadiah Islami Hampers",
        "Al Quran Untuk Wakaf Al Aqeel A5 Kertas Koran 18 Baris | SURABAYA | Alquran Hadiah Islami Hampers",
        "Alquran Edisi Tahlilan Lebih Mulia Daripada Buku Yasin Biasa | Al Aqeel A6 Kertas HVS | SURABAYA |",
        "Paket Wakaf Murah 50 pcs Alquran Al Aqeel | Alquran 18 Baris",
        "PAKET MURAH ALQURAN AL AQEEL MUSHAF NON TERJEMAHAN | SURABAYA | al quran Wakaf/Shodaqoh hadiah hampers islami"
    ]
    produk_khusus = [re.sub(r'\s+', ' ', name.replace('\xa0', ' ')).strip() for name in produk_khusus]
    
    iklan_data = iklan_final_df[iklan_final_df['Nama Iklan'] != 'TOTAL'][['Nama Iklan', 'Biaya']].copy()

    force_config = {}
    if store_type == "Human Store":
        force_config = {
            "Alquran Cover Emas Kertas HVS Al Aqeel Gold Murah": {
                "variasi": ["A7 SATUAN", "A7 PAKET ISI 3", "A7 PAKET ISI 5", "A7 PAKET ISI 7", "A5 SATUAN", "A5 PAKET ISI 3"],
                "denom": 20
            },
            "AL QUR'AN NON TERJEMAH Al AQEEL A5 KERTAS KORAN WAKAF": {
                "variasi": ["SATUAN", "PAKET ISI 3", "PAKET ISI 5", "PAKET ISI 7"],
                "denom": 16
            }
        }
    elif store_type == "Pacific Bookstore":
        force_config = {
            "Alquran GOLD Hard Cover Al Aqeel Kertas HVS | SURABAYA | Alquran untuk Pengajian Wakaf Hadiah Islami Hampers": {
                "variasi": ["A5 Gold Satuan", "A5 Gold Paket isi 3", "A7 Gold Satuan", "A7 Gold Paket isi 3", "A7 Gold Paket isi 5", "A7 Gold Paket isi 7"],
                "denom": 20
            }
        }

    for produk_base, config in force_config.items():
        summary_df['Nama Produk Clean'] = summary_df['Nama Produk'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
        
        matching_ads = iklan_data[iklan_data['Nama Iklan'].str.contains(produk_base, case=False, na=False, regex=False)]
        
        if not matching_ads.empty:
            total_biaya_iklan = matching_ads['Biaya'].sum()
            denom = config['denom']
            
            for var in config['variasi']:
                nama_lengkap_search = f"{produk_base} ({var})".replace('  ', ' ').strip()
                
                exists = summary_df['Nama Produk Clean'].str.contains(re.escape(nama_lengkap_search), case=False, na=False).any()
                
                if not exists:
                    new_row = pd.DataFrame([{col: 0 for col in summary_df.columns}])
                    new_row['Nama Produk'] = f"{produk_base} ({var})"
                    summary_df = pd.concat([summary_df, new_row], ignore_index=True)
                    summary_df['Nama Produk Clean'] = summary_df['Nama Produk'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()

            mask_summary = summary_df['Nama Produk'].str.contains(produk_base, case=False, na=False, regex=False)
            indices = summary_df[mask_summary].index
            
            for idx in indices:
                p_name = summary_df.at[idx, 'Nama Produk']
                count_same = (summary_df['Nama Produk'] == p_name).sum()
                mult = get_eksemplar_multiplier(p_name)
                
                summary_df.at[idx, 'Iklan Klik'] = (mult * total_biaya_iklan) / denom / count_same
            
            iklan_data = iklan_data[~iklan_data['Nama Iklan'].str.contains(produk_base, case=False, na=False, regex=False)]
    summary_df.drop(columns=['Nama Produk Clean'], inplace=True, errors='ignore')

    produk_khusus_biasa = [
        "Paket Alquran Khusus Wakaf Al Aqeel A5 Kertas Koran",
        "AL QUR'AN A6 NON TERJEMAH HVS WARNA PASTEL",
        "Alquran Edisi Tahlilan Lebih Mulia Daripada Buku Yasin Biasa",
        "Al Quran Saku Pastel Al Aqeel A6 Kertas HVS | SURABAYA | Alquran Untuk Wakaf Hadiah Islami Hampers",
        "Al Quran Untuk Wakaf Al Aqeel A5 Kertas Koran 18 Baris | SURABAYA | Alquran Hadiah Islami Hampers",
        "Paket Wakaf Murah 50 pcs Alquran Al Aqeel | Alquran 18 Baris",
        "PAKET MURAH ALQURAN AL AQEEL MUSHAF NON TERJEMAHAN | SURABAYA | al quran Wakaf/Shodaqoh hadiah hampers islami",
        "Alquran Edisi Tahlilan Lebih Mulia Daripada Buku Yasin Biasa | Al Aqeel A6 Kertas HVS | SURABAYA |",
        "Alquran Cover Emas Kertas HVS Al Aqeel A5 Gold Murah", 
        "Alquran Cover Emas Kertas HVS Al Aqeel A7 Gold Murah"
    ]
    
    for p_biasa in produk_khusus_biasa:
        matching_ads = iklan_data[iklan_data['Nama Iklan'].str.contains(p_biasa, case=False, na=False, regex=False)]
        if not matching_ads.empty:
            total_biaya = matching_ads['Biaya'].sum()
            mask_summary = summary_df['Nama Produk'].str.contains(p_biasa, case=False, na=False, regex=False)
            num_rows = mask_summary.sum()
            if num_rows > 0:
                summary_df.loc[mask_summary, 'Iklan Klik'] = total_biaya / num_rows
            else:
                new_row_ads = pd.DataFrame([{col: 0 for col in summary_df.columns}])
                new_row_ads['Nama Produk'] = p_biasa
                new_row_ads['Iklan Klik'] = total_biaya
                summary_df = pd.concat([summary_df, new_row_ads], ignore_index=True)
            iklan_data = iklan_data[~iklan_data['Nama Iklan'].str.contains(p_biasa, case=False, na=False, regex=False)]
    
    summary_df = pd.merge(summary_df, iklan_data, left_on='Nama Produk', right_on='Nama Iklan', how='left')
    
    summary_df['Iklan Klik'] = summary_df['Iklan Klik'] + summary_df['Biaya'].fillna(0)
    summary_df.drop(columns=['Nama Iklan', 'Biaya'], inplace=True, errors='ignore')
    
    iklan_only_names = set(iklan_data['Nama Iklan']) - set(summary_df['Nama Produk'])
    if iklan_only_names:
        iklan_only_df = iklan_data[iklan_data['Nama Iklan'].isin(iklan_only_names)].copy()
        iklan_only_df.rename(columns={'Nama Iklan': 'Nama Produk', 'Biaya': 'Iklan Klik'}, inplace=True)
        summary_df = pd.concat([summary_df, iklan_only_df], ignore_index=True)
    
    summary_df.fillna(0, inplace=True)

    if store_type in ['Pacific Bookstore']:
        summary_df['Penjualan Netto'] = (
            summary_df['Total Harga Produk'] - summary_df['Voucher Ditanggung Penjual'] -
            summary_df['Biaya Komisi AMS + PPN Shopee'] - summary_df['Biaya Adm 8%'] -
            summary_df['Biaya Layanan 4,5%'] - summary_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] -
            summary_df['Biaya Proses Pesanan']
        )
    else:
        summary_df['Penjualan Netto'] = summary_df['Total Penghasilan']
        
    summary_df['Biaya Packing'] = summary_df['Jumlah Terjual'] * 200

    summary_df['Jumlah Eksemplar'] = summary_df.apply(
        lambda row: calculate_eksemplar(row['Nama Produk'], row['Jumlah Terjual']), 
        axis=1
    )

    if store_type in ['Pacific Bookstore']:
        summary_df['Biaya Kirim ke Sby'] = 0
        biaya_ekspedisi_final = summary_df['Biaya Kirim ke Sby']
    else:
        summary_df['Biaya Ekspedisi'] = 0
        biaya_ekspedisi_final = summary_df['Biaya Ekspedisi']

    summary_df['Harga Beli'] = summary_df['Nama Produk'].apply(
        lambda x: get_harga_beli_fuzzy(x, katalog_df)
    )

    summary_df['temp_lookup_key'] = summary_df['Nama Produk'].astype(str).str.replace(' (', ' ', regex=False).str.replace(')', '', regex=False).str.strip()
    
    summary_df = pd.merge(
        summary_df,
        harga_custom_tlj_df[['LOOKUP_KEY', 'HARGA CUSTOM TLJ']],
        left_on='temp_lookup_key',
        right_on='LOOKUP_KEY',
        how='left'
    )
    summary_df.rename(columns={'HARGA CUSTOM TLJ': 'Harga Custom TLJ'}, inplace=True)
    summary_df['Harga Custom TLJ'] = summary_df['Harga Custom TLJ'].fillna(0)
    summary_df.drop(columns=['LOOKUP_KEY', 'temp_lookup_key'], inplace=True, errors='ignore')

    produk_custom_list = ["CUSTOM AL QURAN MENGENANG/WAFAT 40/100/1000 HARI", "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan (Custom sisipan 1 hal)", 
                         "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan (Custom sisipan 2 hal)", "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan (Custom jacket)", 
                         "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan (Custom case)", "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan (Sisipan 1hal+jaket)"]
    
    produk_custom_regex = '|'.join(re.escape(s) for s in produk_custom_list)

    kondisi_custom = summary_df['Nama Produk'].str.contains(produk_custom_regex, na=False)
    
    summary_df['Total Pembelian'] = np.where(
        kondisi_custom,
        (summary_df['Jumlah Terjual'] * summary_df['Harga Beli']) + (summary_df['Jumlah Terjual'] * summary_df['Harga Custom TLJ']),
        summary_df['Jumlah Terjual'] * summary_df['Harga Beli']
    )
    
    summary_df['Margin'] = (
        summary_df['Penjualan Netto'] - summary_df['Iklan Klik'] - summary_df['Biaya Packing'] - 
        biaya_ekspedisi_final - summary_df['Total Pembelian']
    )
    
    summary_df['Persentase'] = (summary_df.apply(lambda row: row['Margin'] / row['Total Harga Produk'] if row['Total Harga Produk'] != 0 else 0, axis=1))
    summary_df['Jumlah Pesanan'] = summary_df.apply(lambda row: row['Biaya Proses Pesanan'] / 1250 if 1250 != 0 else 0, axis=1)
    summary_df['Penjualan Per Hari'] = round(summary_df['Total Harga Produk'] / 7, 1)
    summary_df['Jumlah buku per pesanan'] = round(summary_df.apply(lambda row: row['Jumlah Eksemplar'] / row['Jumlah Pesanan'] if row.get('Jumlah Pesanan', 0) != 0 else 0, axis=1), 1)
    
    summary_final_data = {
        'No': np.arange(1, len(summary_df) + 1), 'Nama Produk': summary_df['Nama Produk'],
        'Jumlah Terjual': summary_df['Jumlah Terjual'], 'Jumlah Eksemplar': summary_df['Jumlah Eksemplar'], 
        'Jumlah Pesanan': summary_df['Jumlah Pesanan'], 'Harga Satuan': summary_df['Harga Satuan'],
        'Total Penjualan': summary_df['Total Harga Produk'], 'Voucher Ditanggung Penjual': summary_df['Voucher Ditanggung Penjual'],
        'Biaya Komisi AMS + PPN Shopee': summary_df['Biaya Komisi AMS + PPN Shopee'], 'Biaya Adm 8%': summary_df['Biaya Adm 8%'],
        biaya_layanan_col: summary_df[biaya_layanan_col], 'Biaya Layanan Gratis Ongkir Xtra 4,5%': summary_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'],
        'Biaya Proses Pesanan': summary_df['Biaya Proses Pesanan'],
        'Penjualan Netto': summary_df['Penjualan Netto'], 'Iklan Klik': summary_df['Iklan Klik'], 'Biaya Packing': summary_df['Biaya Packing'],
    }
    if store_type in ['Pacific Bookstore']:
        summary_final_data['Biaya Ekspedisi'] = biaya_ekspedisi_final
    else:
        summary_final_data['Biaya Ekspedisi'] = biaya_ekspedisi_final
    summary_final_data.update({
        'Harga Beli': summary_df['Harga Beli'], 'Harga Custom TLJ': summary_df['Harga Custom TLJ'],
        'Total Pembelian': summary_df['Total Pembelian'], 'Margin': summary_df['Margin'],
        'Persentase': summary_df['Persentase'],
        'Penjualan Per Hari': summary_df['Penjualan Per Hari'], 'Jumlah buku per pesanan': summary_df['Jumlah buku per pesanan']
    })
    summary_final = pd.DataFrame(summary_final_data)

    mapping_singkatan = {}
    if store_type == "Human Store":
        mapping_singkatan = {
            "AL-QUR'AN TERJEMAH HC AL ALEEM QPP A6": "Al Aleem A6 QPP",
            "AL-QUR'AN TERJEMAH  HC AL ALEEM QPP A6": "Al Aleem A6 QPP",
            "AL-QURAN AL AQEEL SILVER TERMURAH": "Al Aqeel Silver",
            "Paket Wakaf Murah 50 pcs Alquran Al Aqeel | Alquran 18 Baris": "Paket Wakaf Murah Al Aqeel 50 pcs",
            "AL QUR'AN WAQF IBTIDA | AL QUDDUS A5 KERTAS HVS": "Al Quddus A5 HVS",
            "AL QUR'AN AL AQEEL B5 KERTAS HVS": "Al Aqeel B5 HVS",
            "KAMUS BERGAMBAR 3 BAHASA - INDONESIA INGGRIS ARAB": "Kamus Bergambar 3 Bahasa",
            "AL QUR'AN NON TERJEMAH Al AQEEL A5 KERTAS KORAN WAKAF": "AL AQEEL A5 KORAN",
            "Paket Alquran Khusus Wakaf Al Aqeel A5 Kertas Koran | Alquran Murah Kualitas Terbaik Harga Ekonomis | Jakarta": "Al Aqeel A5 Koran",
            "Al QUR'AN NON TERJEMAH AL AQEEL KERTAS KORAN B5 WAKAF": "Al Aqeel B5 Koran",
            "Alquran Cover Emas Kertas HVS Al Aqeel Gold Murah": "Al Aqeel Gold",
            "AL-QUR'AN TERJEMAH HC AL ALEEM A5": "Al Aleem A5",
            "Komik Pahlawan, Pendidikan Sejarah Untuk Anak": "Komik Pahlawan",
            "AL QUR'AN AL FIKRAH TERJEMAH PER AYAT PER KATA A4 KERTAS HVS": "Al Fikrah A4 HVS",
            "AL QUR'AN HAFALAN SAKU A7 MAHEER KERTAS QPP": "A7 Maheer QPP",
            "AL QUR'AN B5 NON TERJEMAH HVS WARNA PASTEL": "Al Aqeel B5 Pastel",
            "AL QURAN SAKU RESLETING A7 AL QUDDUS KERTAS QPP": "Al Quddus A7 Saku QPP",
            "BUKU CERITA ANAK FABEL SERI DONGENG BINATANG DUA BAHASA": "Fabel Binatang",
            "BUKU CERITA KISAH TELADAN NABI SERI VOL 1-6": "Kisah Teladan Nabi",
            "AL- QUR'AN TAJWID WARNA WAQF IBTIDA | SUBHAAN A5 KERTAS QPP": "Subhaan A5 QPP",
            "BUKU LAGU HARMONI NUSANTARA LAGU NASIONAL & DAERAH": "Buku Lagu Harmoni Nusantara",
            "[KOLEKSI TERBARU] SERI CERITA RAKYAT": "Seri Cerita Rakyat",
            "[KOLEKSI TERBARU] BUKU CERITA ANAK SERI BUDI PEKERTI": "Seri Budi Pekerti",
            "AL- QUR'AN TERJEMAH TAJWID MUMTAAZ A5 KERTAS QPP": "Mumtaaz A5 QPP",
            "AL QUR'AN A6 NON TERJEMAH HVS WARNA PASTEL": "Al Aqeel 6 Pastel",
            "Custom Al Quran Mengenang/Wafat 40/100/1000 Hari": "Alquran Custom",
            "AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan": "A6 edisi Tahlilan",
            "Al-Qur'an Non Terjemah Al Aqeel HVS A5": "Al Aqeel A5 HVS",
            "Al Qur'an Terjemah Per Kata | Tajwid 2 Warna | Al Fikrah A5 Kertas HVS": "Al Fikrah A5 HVS"
        }
    elif store_type == "Pacific Bookstore":
        mapping_singkatan = {
            "Alquran Custom Nama Foto | SURABAYA | Al-Quran untuk Wakaf Tasyakuran Tahlil Yasin Hadiah Hampers Islami": "Alquran Custom Al Aqeel",
            "PAKET MURAH ALQURAN AL AQEEL MUSHAF NON TERJEMAHAN | SURABAYA | al quran Wakaf/Shodaqoh hadiah hampers islami": "PAKET MURAH AL AQEEL MIN 10 EKS",
            "Al Quran Terjemah Per Kata A5 | Tajwid 2 Warna | Alquran Al Fikrah HVS 15 Baris | SURABAYA": "Al Fikrah A5 HVS",
            "Alquran GOLD Hard Cover Al Aqeel Kertas HVS | SURABAYA | Alquran untuk Pengajian Wakaf Hadiah Islami Hampers": "Al Aqeel Gold Kertas HVS",
            "Al Quran Untuk Wakaf Al Aqeel A5 Kertas Koran 18 Baris | SURABAYA | Alquran Hadiah Islami Hampers": "Al Aqeel A5 Kertas Koran",
            "Al Quran Saku Pastel Al Aqeel A6 Kertas HVS | SURABAYA | Alquran Untuk Wakaf Hadiah Islami Hampers": "Al Aqeel A6 Kertas HVS",
            "Alquran Edisi Tahlilan Lebih Mulia Daripada Buku Yasin Biasa | Al Aqeel A6 Kertas HVS | SURABAYA |": "Al Aqeel A6 Edisi Tahlilan Kertas HVS",
            "Alquran Edisi Tahlilan Lebih Mulia Daripada Buku Yasin Biasa": "Al Aqeel A6 Edisi Tahlilan Kertas HVS",
            "Al Quran Saku Resleting Al Quddus A7 Cover Kulit Kertas QPP | Alquran SURABAYA": "Al Quddus A7 Cover Kulit Kertas QPP",
            "Al Quran Saku Resleting Al Quddus A7 QPP Cover Kulit | SURABAYA | Untuk Santri Traveler Muslim": "Al Quddus A7 Cover Kulit Kertas QPP",
            "Al Quran Terjemah Al Aleem A5 Kertas HVS 15 Baris | SURABAYA | Alquran Untuk Majelis Taklim Kajian": "Al Aleem A5 Kertas HVS",
            "Al Quran Wakaf Ibtida Al Quddus A5 Kertas HVS | Alquran SURABAYA": "Al Quddus Ibtida A5 Kertas HVS"
        }

    if mapping_singkatan:
        def apply_shorten(nama_full):
            if pd.isna(nama_full): return nama_full
            match_variasi = re.search(r'(\s*\(.*\))$', nama_full)
            variasi_part = match_variasi.group(1) if match_variasi else ""
            nama_produk_saja = nama_full.replace(variasi_part, "").strip()

            for original_name, short_name in mapping_singkatan.items():
                if original_name.lower() in nama_produk_saja.lower():
                    return f"{short_name}{variasi_part}"
            return nama_full

        summary_final['Nama Produk'] = summary_final['Nama Produk'].apply(apply_shorten)
        
    summary_final = summary_final.sort_values(by='Nama Produk', ascending=True).reset_index(drop=True)
    summary_final['No'] = range(1, len(summary_final) + 1)
    
    total_row = pd.DataFrame(summary_final.sum(numeric_only=True)).T
    total_row['Nama Produk'] = 'Total'
    total_penjualan_netto = total_row['Penjualan Netto'].iloc[0]
    total_iklan_klik = total_row['Iklan Klik'].iloc[0]
    total_biaya_packing = total_row['Biaya Packing'].iloc[0]
    total_pembelian = total_row['Total Pembelian'].iloc[0]
    total_harga_produk = total_row['Total Penjualan'].iloc[0]
    total_biaya_proses_pesanan = total_row['Biaya Proses Pesanan'].iloc[0]
    total_jumlah_terjual = total_row['Jumlah Terjual'].iloc[0]
    total_jumlah_eksemplar = total_row['Jumlah Eksemplar'].iloc[0]
    biaya_ekspedisi_col_name = 'Biaya Ekspedisi' if store_type == 'Pacific Bookstore' else 'Biaya Ekspedisi'
    total_biaya_ekspedisi = total_row[biaya_ekspedisi_col_name].iloc[0]
    total_margin = total_penjualan_netto - total_biaya_packing - total_biaya_ekspedisi - total_pembelian - total_iklan_klik
    total_row['Margin'] = total_margin
    total_row['Persentase'] = (total_margin / total_harga_produk) if total_harga_produk != 0 else 0
    total_jumlah_pesanan = (total_biaya_proses_pesanan / 1250) if 1250 != 0 else 0
    total_row['Jumlah Pesanan'] = total_jumlah_pesanan
    total_row['Penjualan Per Hari'] = round(total_harga_produk / 7, 1)
    total_row['Jumlah buku per pesanan'] = round(total_jumlah_eksemplar / total_jumlah_pesanan if total_jumlah_pesanan != 0 else 0, 1)
    for col in ['Harga Satuan', 'Harga Beli', 'No', 'Harga Custom TLJ']:
        if col in total_row.columns: total_row[col] = None
    summary_with_total = pd.concat([summary_final, total_row], ignore_index=True)
    
    return summary_with_total

def format_variation_dama(variation, product_name):
    """Format variasi untuk DAMA.ID STORE SUMMARY."""
    if pd.isna(variation):
        return ''

    var_str = str(variation).strip()
    if var_str == '0':
        return ''

    product_name_upper = str(product_name).upper()

    color_keywords = {'merah', 'biru', 'hijau', 'kuning', 'hitam', 'putih', 'ungu', 'coklat', 'cokelat',
                      'abu', 'pink', 'gold', 'silver', 'cream', 'navy', 'maroon', 'random',
                      'army', 'olive', 'mocca', 'dusty', 'sage'}
    hijab_keywords = {'PIRING', 'BAJU', 'MOBIL'}
    keep_keywords = {'HVS', 'QPP', 'KORAN', 'KK', 'KWARTO', 'BIGBOS', 'ART PAPER'}
    keep_patterns = [r'\b(PAKET\s*\d+)\b', r'\b((A|B)\d{1,2})\b']

    keep_color = any(keyword in product_name_upper for keyword in hijab_keywords)

    parts = re.split(r'[\s,]+', var_str)
    final_parts = []

    for part in parts:
        part_upper = part.upper()
        part_lower = part.lower()

        if not part or part == '0':
            continue

        is_color = part_lower in color_keywords

        if not is_color or (is_color and keep_color):
            is_kept_keyword = part_upper in keep_keywords
            is_kept_pattern = any(re.fullmatch(pattern, part_upper) for pattern in keep_patterns)

            if not is_color or keep_color or is_kept_keyword or is_kept_pattern:
                 final_parts.append('KORAN' if part_upper == 'KK' else part)

    unique_parts_ordered = list(dict.fromkeys(final_parts))

    return ' '.join(unique_parts_ordered)

def get_harga_beli_dama(summary_product_name, katalog_dama_df, score_threshold_primary=80, score_threshold_fallback=75):
    """Mencari harga beli dari KATALOG_DAMA."""
    try:
        if pd.isna(summary_product_name) or not summary_product_name.strip():
            return 0

        base_name = summary_product_name.strip()
        variasi_part = ''
        match = re.match(r'^(.*?)\s*\((.*?)\)$', summary_product_name.strip())
        if match:
            base_name = match.group(1).strip()
            variasi_part = match.group(2).strip().upper()

        base_name_upper_clean = re.sub(r'\s+', ' ', base_name.upper()).strip()

        ukuran_in_var = ''
        jenis_in_var = ''
        paket_in_var = ''

        size_match = re.search(r'\b((A|B)\d{1,2})\b', variasi_part)
        if size_match: ukuran_in_var = size_match.group(1)

        paper_keywords = {'HVS', 'QPP', 'KORAN', 'KK', 'KWARTO', 'BIGBOS', 'ART PAPER'}
        variasi_words = set(re.split(r'\s+', variasi_part))
        for paper in paper_keywords:
            if paper in variasi_words:
                jenis_in_var = 'KORAN' if paper == 'KK' else paper
                break

        package_match = re.search(r'\b(PAKET\s*\d+)\b', variasi_part)
        if package_match: 
            paket_in_var = re.sub(r'\s+', ' ', package_match.group(1)).strip()
        
        warna_in_var = ''
        color_keywords_set = {'MERAH', 'BIRU', 'HIJAU', 'KUNING', 'HITAM', 'PUTIH', 'UNGU', 'COKLAT', 'COKELAT',
                              'ABU', 'PINK', 'GOLD', 'SILVER', 'CREAM', 'NAVY', 'MAROON', 'RANDOM',
                              'ARMY', 'OLIVE', 'MOCCA', 'DUSTY', 'SAGE'}
        found_colors = variasi_words.intersection(color_keywords_set)
        if found_colors:
            warna_in_var = list(found_colors)[0]
        
        hijab_keywords = {'PASHMINA', 'HIJAB', 'PASMINA'}
        match_warna_required = any(keyword in base_name_upper_clean for keyword in hijab_keywords)

        best_strict_score = -1
        best_strict_price = 0
        
        best_fallback_score = -1
        best_fallback_price = 0

        for index, row in katalog_dama_df.iterrows():
            katalog_name = row['NAMA PRODUK']
            katalog_jenis = row['JENIS AL QUR\'AN']
            katalog_ukuran = row['UKURAN']
            katalog_paket = row['PAKET']
            katalog_warna = row['WARNA']
            
            name_score = fuzz.token_set_ratio(base_name_upper_clean, katalog_name)

            if name_score >= score_threshold_primary:
                match_ok = True

                if jenis_in_var and katalog_jenis != jenis_in_var:
                    match_ok = False
                if ukuran_in_var and katalog_ukuran != ukuran_in_var:
                    match_ok = False
                
                if paket_in_var != katalog_paket:
                    match_ok = False

                if match_warna_required:
                    if katalog_warna != warna_in_var:
                        match_ok = False

                if match_ok:
                    if name_score > best_strict_score:
                        best_strict_score = name_score
                        best_strict_price = row['HARGA']

            if name_score >= score_threshold_fallback:
                if name_score > best_fallback_score:
                    best_fallback_score = name_score
                    best_fallback_price = row['HARGA']

        if best_strict_score != -1:
            return best_strict_price
        
        if best_fallback_score != -1:
            return best_fallback_price

        return 0
    except Exception:
        return 0

def get_eksemplar_multiplier_dama(nama_produk):
    if pd.isna(nama_produk): return 1
    nama_produk = str(nama_produk).upper()
    if 'BIGBOS' in nama_produk:
        return 1
    match = re.search(r'(?:PAKET\s*ISI|PAKET|ISI)\s*(\d+)', nama_produk)
    if match:
        return int(match.group(1))
    if 'SATUAN' in nama_produk:
        return 1
    return 1
    
def process_summary_dama(rekap_df, iklan_final_df, katalog_dama_df, harga_custom_tlj_df):
    """Fungsi untuk memproses sheet 'SUMMARY' untuk DAMA.ID STORE."""
    rekap_copy = rekap_df.copy()
    rekap_copy['No. Pesanan'] = rekap_copy['No. Pesanan'].replace('', np.nan).ffill()

    kondisi_retur_summary = rekap_copy['Total Penghasilan'] <= 0
    
    rekap_copy.loc[kondisi_retur_summary, 'Jumlah Terjual'] = 0
    rekap_copy.loc[kondisi_retur_summary, 'Total Harga Produk'] = 0

    rekap_copy['Nama Produk Original'] = rekap_copy['Nama Produk']
    if 'Nama Variasi' in rekap_copy.columns:
        rekap_copy['Formatted Variation'] = rekap_copy.apply(
            lambda row: format_variation_dama(row['Nama Variasi'], row['Nama Produk Original']),
            axis=1
        )
        rekap_copy['Nama Produk Display'] = rekap_copy.apply(
            lambda row: f"{row['Nama Produk Original']} ({row['Formatted Variation']})"
                        if row['Formatted Variation'] else row['Nama Produk Original'],
            axis=1
        )
    else:
         rekap_copy['Nama Produk Display'] = rekap_copy['Nama Produk Original']
         rekap_copy['Formatted Variation'] = ''

    grouping_key_list = ['Nama Produk Display', 'Harga Satuan']
    
    agg_dict = {
        'Nama Produk Original': 'first',
        'Nama Produk Display': 'first',
        'Jumlah Terjual': 'sum', 
        'Total Harga Produk': 'sum',
        'Voucher Ditanggung Penjual': 'sum', 'Biaya Komisi AMS + PPN Shopee': 'sum',
        'Biaya Adm 8%': 'sum', 'Biaya Layanan 2%': 'sum',
        'Biaya Layanan Gratis Ongkir Xtra 4,5%': 'sum', 'Biaya Proses Pesanan': 'sum',
        'Total Penghasilan': 'sum'
    }
    
    summary_df = rekap_copy.groupby(grouping_key_list, as_index=False).agg(agg_dict)
    summary_df.rename(columns={'Nama Produk Display': 'Nama Produk'}, inplace=True)
    
    summary_df = summary_df[summary_df['Total Penghasilan'] != 0].copy()

    summary_df['Iklan Klik'] = 0.0
    produk_khusus_raw = ["CUSTOM AL QURAN MENGENANG/WAFAT 40/100/1000 HARI", "Paket Hemat Paket Al Quran | AQ Al Aqeel Wakaf Kerta koran Non Terjemah", "Alquran Al Aqeel A5 Kertas Koran Tanpa Terjemahan Wakaf Ibtida"]
    produk_khusus = [re.sub(r'\s+', ' ', name.replace('\xa0', ' ')).strip() for name in produk_khusus_raw]
    iklan_data = iklan_final_df[iklan_final_df['Nama Iklan'] != 'TOTAL'][['Nama Iklan', 'Biaya']].copy()
    
    force_config_dama = {
        "Al Quran Wakaf Saku A6 Al Aqeel HVS Paket Wakaf": {
            "variasi": ["SATUAN", "PAKET ISI 3", "PAKET ISI 5", "PAKET ISI 7"],
            "denom": 16
        },
        "Al Quran Gold Silver Al Aqeel Besar Sedang Kecil": {
            "variasi": ["A4 Satuan", "B5 Satuan", "A7 Satuan", "A6 Satuan", "A5 Satuan", "A7 Paket isi 3", "A7 Paket isi 5", "A7 Paket isi 7", "A5 Paket isi 3"],
            "denom": 23
        }
    }

    for produk_base, config in force_config_dama.items():
        summary_df['Nama Produk Clean'] = summary_df['Nama Produk'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
        
        matching_ads = iklan_data[iklan_data['Nama Iklan'].str.contains(produk_base, case=False, na=False, regex=False)]
        if not matching_ads.empty:
            total_biaya_iklan = matching_ads['Biaya'].sum()
            denom = config['denom']
            
            for var in config['variasi']:
                nama_lengkap_search = f"{produk_base} ({var})".replace('  ', ' ').strip()
                exists = summary_df['Nama Produk Clean'].str.contains(re.escape(nama_lengkap_search), case=False, na=False).any()
                
                if not exists:
                    new_row = pd.DataFrame([{col: 0 for col in summary_df.columns}])
                    new_row['Nama Produk'] = f"{produk_base} ({var})"
                    summary_df = pd.concat([summary_df, new_row], ignore_index=True)
                    summary_df['Nama Produk Clean'] = summary_df['Nama Produk'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
            
            mask_summary = summary_df['Nama Produk'].str.contains(produk_base, case=False, na=False, regex=False)
            indices = summary_df[mask_summary].index
            
            for idx in indices:
                p_name = summary_df.at[idx, 'Nama Produk']
                count_same = (summary_df['Nama Produk'] == p_name).sum()
                mult = get_eksemplar_multiplier_dama(p_name)
                summary_df.at[idx, 'Iklan Klik'] = (mult * total_biaya_iklan) / denom / count_same
            
            iklan_data = iklan_data[~iklan_data['Nama Iklan'].str.contains(produk_base, case=False, na=False, regex=False)]

    summary_df.drop(columns=['Nama Produk Clean'], inplace=True, errors='ignore')

    if not iklan_data.empty:
        p_tahlil = "ALQURAN SAKU A6 EDISI TAHLIL TERBARU"
        matching_ads = iklan_data[iklan_data['Nama Iklan'].str.contains(p_tahlil, case=False, na=False, regex=False)]
        if not matching_ads.empty:
            total_biaya = matching_ads['Biaya'].sum()
            mask_summary = summary_df['Nama Produk'].str.contains(p_tahlil, case=False, na=False, regex=False)
            num_rows = mask_summary.sum()
            if num_rows > 0:
                summary_df.loc[mask_summary, 'Iklan Klik'] = total_biaya / num_rows
            else:
                new_row_ads = pd.DataFrame([{col: 0 for col in summary_df.columns}])
                new_row_ads['Nama Produk'] = p_tahlil
                new_row_ads['Iklan Klik'] = total_biaya
                summary_df = pd.concat([summary_df, new_row_ads], ignore_index=True)
            iklan_data = iklan_data[~iklan_data['Nama Iklan'].str.contains(p_tahlil, case=False, na=False, regex=False)]
                
    summary_df = pd.merge(summary_df, iklan_data, left_on='Nama Produk Original', right_on='Nama Iklan', how='left')
    summary_df['Iklan Klik'] = summary_df['Iklan Klik'] + summary_df['Biaya'].fillna(0)
    summary_df.drop(columns=['Nama Iklan', 'Biaya'], inplace=True, errors='ignore')

    iklan_only_names = set(iklan_data['Nama Iklan']) - set(summary_df['Nama Produk Original'])
    if iklan_only_names:
        iklan_only_df = iklan_data[iklan_data['Nama Iklan'].isin(iklan_only_names)].copy()
        iklan_only_df.rename(columns={'Nama Iklan': 'Nama Produk', 'Biaya': 'Iklan Klik'}, inplace=True)
        iklan_only_df['Nama Produk Original'] = iklan_only_df['Nama Produk']
        summary_df = pd.concat([summary_df, iklan_only_df], ignore_index=True)
    summary_df.fillna(0, inplace=True)

    summary_df['Penjualan Netto'] = (
        summary_df['Total Harga Produk'] - summary_df['Voucher Ditanggung Penjual'] -
        summary_df['Biaya Komisi AMS + PPN Shopee'] - summary_df['Biaya Adm 8%'] -
        summary_df['Biaya Layanan 2%'] - summary_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'] -
        summary_df['Biaya Proses Pesanan']
    )
    summary_df['Biaya Packing'] = summary_df['Jumlah Terjual'] * 200

    summary_df['Jumlah Eksemplar'] = summary_df.apply(
        lambda row: row['Jumlah Terjual'] * get_eksemplar_multiplier_dama(row['Nama Produk']),
        axis=1
    )
    
    hijab_keywords_dama = {'PIRING', 'BAJU', 'MOBIL'}
    kondisi_hijab = summary_df['Nama Produk Original'].str.upper().str.contains('|'.join(hijab_keywords_dama), na=False)
    summary_df.loc[kondisi_hijab, 'Jumlah Eksemplar'] = 0
    
    summary_df['Biaya Ekspedisi'] = 0
    biaya_ekspedisi_final = summary_df['Biaya Ekspedisi']

    summary_df['Harga Beli'] = summary_df['Nama Produk'].apply(
        lambda x: get_harga_beli_dama(x, katalog_dama_df)
    )

    summary_df = pd.merge(
        summary_df,
        harga_custom_tlj_df[['LOOKUP_KEY', 'HARGA CUSTOM TLJ']],
        left_on='Nama Produk',
        right_on='LOOKUP_KEY', how='left'
    )
    summary_df.rename(columns={'HARGA CUSTOM TLJ': 'Harga Custom TLJ'}, inplace=True)
    summary_df['Harga Custom TLJ'] = summary_df['Harga Custom TLJ'].fillna(0)
    summary_df.drop(columns=['LOOKUP_KEY'], inplace=True, errors='ignore')

    produk_custom_str = "CUSTOM AL QURAN MENGENANG/WAFAT 40/100/1000 HARI"
    kondisi_custom = summary_df['Nama Produk Original'].str.contains(produk_custom_str, na=False)
    summary_df['Total Pembelian'] = np.where(
        kondisi_custom,
        (summary_df['Jumlah Terjual'] * summary_df['Harga Beli']) + (summary_df['Jumlah Terjual'] * summary_df['Harga Custom TLJ']),
        summary_df['Jumlah Terjual'] * summary_df['Harga Beli']
    )

    summary_df['Margin'] = (
        summary_df['Penjualan Netto'] - summary_df['Iklan Klik'] - summary_df['Biaya Packing'] -
        biaya_ekspedisi_final - summary_df['Total Pembelian']
    )

    summary_df['Persentase'] = (summary_df.apply(lambda row: row['Margin'] / row['Total Harga Produk'] if row['Total Harga Produk'] != 0 else 0, axis=1))
    summary_df['Jumlah Pesanan'] = summary_df.apply(lambda row: row['Biaya Proses Pesanan'] / 1250 if 1250 != 0 else 0, axis=1)
    summary_df['Penjualan Per Hari'] = round(summary_df['Total Harga Produk'] / 7, 1)
    summary_df['Jumlah buku per pesanan'] = round(summary_df.apply(lambda row: row['Jumlah Eksemplar'] / row['Jumlah Pesanan'] if row.get('Jumlah Pesanan', 0) != 0 else 0, axis=1), 1)

    summary_final_data = {
        'No': np.arange(1, len(summary_df) + 1),
        'Nama Produk': summary_df['Nama Produk'],
        'Jumlah Terjual': summary_df['Jumlah Terjual'], 'Jumlah Eksemplar': summary_df['Jumlah Eksemplar'], 
        'Jumlah Pesanan': summary_df['Jumlah Pesanan'], 'Harga Satuan': summary_df['Harga Satuan'],
        'Total Penjualan': summary_df['Total Harga Produk'], 'Voucher Ditanggung Penjual': summary_df['Voucher Ditanggung Penjual'],
        'Biaya Komisi AMS + PPN Shopee': summary_df['Biaya Komisi AMS + PPN Shopee'], 'Biaya Adm 8%': summary_df['Biaya Adm 8%'],
        'Biaya Layanan 2%': summary_df['Biaya Layanan 2%'], 'Biaya Layanan Gratis Ongkir Xtra 4,5%': summary_df['Biaya Layanan Gratis Ongkir Xtra 4,5%'],
        'Biaya Proses Pesanan': summary_df['Biaya Proses Pesanan'],
        'Penjualan Netto': summary_df['Penjualan Netto'], 'Iklan Klik': summary_df['Iklan Klik'], 'Biaya Packing': summary_df['Biaya Packing'],
        'Biaya Ekspedisi': biaya_ekspedisi_final,
        'Harga Beli': summary_df['Harga Beli'], 'Harga Custom TLJ': summary_df['Harga Custom TLJ'],
        'Total Pembelian': summary_df['Total Pembelian'], 'Margin': summary_df['Margin'],
        'Persentase': summary_df['Persentase'],
        'Penjualan Per Hari': summary_df['Penjualan Per Hari'], 'Jumlah buku per pesanan': summary_df['Jumlah buku per pesanan']
    }
    summary_final = pd.DataFrame(summary_final_data)

    mapping_dama = {
        "Alquran Al Aqeel A5 Kertas Koran Tanpa Terjemahan Wakaf Ibtida": "Al Aqeel A5 Kertas Koran",
        "AL QUR'AN CUSTOM NAMA FOTO DI COVER SISIPAN ACARA TASYAKUR TAHLIL YASIN": "AL QUR'AN CUSTOM COVER SISIPAN",
        "PAKET MURAH Alquran Al-Aqeel Tanpa Terjemahan | BANDUNG | Alquran Wakaf Hadiah Hampers Islami": "PAKET MURAH Al-Aqeel Tanpa Terjemahan",
        "Al Quran Gold Silver Al Aqeel Besar Sedang Kecil": "Al Aqeel Gold Silver",
        "ALQURAN A6 HVS EDISI TAHLIL TERBARU": "al aqeel A6 edisi tahlilan",
        "Al Quran Wakaf Saku A6 Al Aqeel HVS Paket Wakaf": "Al Aqeel A6 HVS",
        "AL QURAN LATIN TERJEMAHAN DAN TADJWID MUSHAF AL FIKRAH KERTAS HVS": "AL FIKRAH A5 HVS",
        "Al Quran Mushaf Al Aqeel Full Color A5 HVS": "Al Aqeel A5 HVS",
        "AL QURAN AL QUDDUS SAKU A7 KULIT RESLETING": "AL QUDDUS SAKU A7 KULIT",
        "BELLA SQUARE PREMIUM | HIJAB SEGIEMPAT | VARIASI WARNA | MURAH FASHION MUSLIM": "HIJAB SEGIEMPAT BELLA SQUARE",
        "Mushaf Al-Qur'an Al Quddus Tanpa terjemahan uk A5 DAN A4": "Al Quddus Tanpa terjemahan uk A5 DAN A4",
        "Juz'amma Edisi Terbaru Lebih Lengkap Terjemahan Tadjwid Asmaul Husna Soft Cover Kertas Koran": "Juz'amma Kertas Koran",
        "HIJAB PASMINA KAOS RAYON COOL TECH BY DAMA": "PASMINA KAOS RAYON",
        "PASHMINA OVAL CERUTY BABYDOLL PREMIUM": "PASHMINA OVAL CERUTY BABYDOLL",
        "BUKU CERITA ANAK SERI BUDI PEKERTI KOBER TK SD": "BUKU CERITA SERI BUDI PEKERTI TK SD",
        "AL QUR'AN TERJEMAHAN AL ALEEM WAQAF IBTIDA": "AL ALEEM WAQAF IBTIDA",
        "AlQuran Mushaf Al Aqeel B5": "Al Aqeel B5 HVS",
        "SERI DONGENG BINATANG | DONGENG FABEL | DONGENG BINATANG MENARIK": "SERI DONGENG BINATANG",
        "Buku Cerita Seri Terladan Nabi Seri 6 Untuk Anak Anak": "Buku Cerita Seri Teladan Nabi",
        "BUKU CERITA SERI CERITA RAKYAT | NUSANTARA": "BUKU CERITA SERI CERITA RAKYAT",
        "AL QUR'AN TADJWID DAN TERJEMAHAN TAFSIR ASBABUNNUZUL WAQAF IBTIDA MUSHAF MUMTAAZ": "AL QUR'AN TADJWID DAN TERJEMAHAN MUMTAAZ WAQAF IBTIDA",
        "Juz'amma Edisi Terbaru Lebih Lengkap Terjemahan Tajwid Asmaul Husnah kertas HVS": "Juz'amma kertas HVS",
        "Kamus Bergambar Bilingual TK SD PAUD": "Kamus Bergambar TK SD PAUD",
        "AL QURAN MUSHAF AL ALEEM A6 SAKU": "AL ALEEM A6 SAKU",
        "HIJAB PAYET CANTIK | PARIS JEPANG | hijab kekinian": "HIJAB PAYET PARIS JEPANG",
        "TERBARU KOMIK SERI PAHLAWAN INDONESIA | BUKU PAHLAWAN": "KOMIK SERI PAHLAWAN INDONESIA",
        "HARMONI NUSANTARA | LAGU NASIONAL DAN LAGU DAERAH INDONESIA": "LAGU NASIONAL DAN LAGU DAERAH INDONESIA",
        "HIJAB BERGO JERSEY BY DAMA | KERUDUNG INSTAN": "HIJAB BERGO JERSEY",
        "HIJAB VOAL MOTIF LASER CUT PREMIUM": "HIJAB VOAL MOTIF LASER CUT",
        "Al QURAN TADJWID TANPA TERJEMAHAN MUSHAF SUBHAAN": "SUBHAAN TADJWID TANPA TERJEMAHAN"
    }

    def apply_shorten_dama(nama_full):
        if pd.isna(nama_full): return nama_full
        nama_full_str = str(nama_full)
        match_variasi = re.search(r'(\s*\(.*\))$', nama_full_str)
        variasi_part = match_variasi.group(1) if match_variasi else ""
        nama_produk_saja = nama_full_str.replace(variasi_part, "").strip()

        for original_name, short_name in mapping_dama.items():
            if original_name.lower() in nama_produk_saja.lower():
                return f"{short_name}{variasi_part}"
        return nama_full_str

    summary_final['Nama Produk'] = summary_final['Nama Produk'].apply(apply_shorten_dama)
    
    summary_final['Nama Produk'] = summary_final['Nama Produk'].astype(str)
    
    summary_final = summary_final.sort_values(by='Nama Produk', ascending=True).reset_index(drop=True)
    summary_final['No'] = range(1, len(summary_final) + 1)

    if 'Nama Produk Original' in summary_final.columns:
         summary_final = summary_final.drop(columns=['Nama Produk Original'])

    total_row = pd.DataFrame(summary_final.sum(numeric_only=True)).T
    total_row['Nama Produk'] = 'Total'
    total_penjualan_netto = total_row['Penjualan Netto'].iloc[0]
    total_iklan_klik = total_row['Iklan Klik'].iloc[0]
    total_biaya_packing = total_row['Biaya Packing'].iloc[0]
    total_pembelian = total_row['Total Pembelian'].iloc[0]
    total_harga_produk = total_row['Total Penjualan'].iloc[0]
    total_biaya_proses_pesanan = total_row['Biaya Proses Pesanan'].iloc[0]
    total_jumlah_terjual = total_row['Jumlah Terjual'].iloc[0]
    total_jumlah_eksemplar = total_row['Jumlah Eksemplar'].iloc[0]
    total_biaya_ekspedisi = total_row['Biaya Ekspedisi'].iloc[0]
    total_margin = total_penjualan_netto - total_biaya_packing - total_biaya_ekspedisi - total_pembelian - total_iklan_klik
    total_row['Margin'] = total_margin
    total_row['Persentase'] = (total_margin / total_harga_produk) if total_harga_produk != 0 else 0
    total_jumlah_pesanan = (total_biaya_proses_pesanan / 1250) if 1250 != 0 else 0
    total_row['Jumlah Pesanan'] = total_jumlah_pesanan
    total_row['Penjualan Per Hari'] = round(total_harga_produk / 7, 1)
    total_row['Jumlah buku per pesanan'] = round(total_jumlah_eksemplar / total_jumlah_pesanan if total_jumlah_pesanan != 0 else 0, 1)
    for col in ['Harga Satuan', 'Harga Beli', 'No', 'Harga Custom TLJ']:
        if col in total_row.columns: total_row[col] = None
    summary_with_total = pd.concat([summary_final, total_row], ignore_index=True)

    return summary_with_total


# ============================================
# FUNGSI-FUNGSI IKLAN HARIAN (DARI IKLANKU.PY)
# ============================================

def clean_nama_iklan(text):
    """Membersihkan nama iklan dari angka dalam kurung."""
    if not isinstance(text, str):
        return str(text)
    return re.sub(r'\s*\[\d+\]\s*$', '', text).strip()

def extract_time_hour(dt):
    """Ekstrak jam dari datetime."""
    try:
        return dt.hour
    except:
        return 0

def extract_eksemplar(variasi_text):
    """Ekstrak jumlah eksemplar dari teks variasi."""
    if not isinstance(variasi_text, str):
        return 1
    
    v = variasi_text.strip().upper()
    
    match = re.search(r'(?:PAKET|ISI)\s*(?:ISI\s*)?(\d+)', v)
    
    if match:
        return int(match.group(1))
    
    return 1

def clean_variasi(text, product_name=""):
    """Membersihkan variasi untuk Shopee."""
    if not isinstance(text, str) or pd.isna(text) or text == '':
        return ''

    is_paket_wakaf = "Paket Wakaf Murah 50 pcs Alquran Al Aqeel | Alquran 18 Baris" in str(product_name)
    
    if is_paket_wakaf:
        part = text.split(',')[0].strip().upper()
        return part.replace('AL AQEEL', '').strip()
   
    if ',' in text:
        parts = text.split(',')
        return parts[-1].strip().upper()
    return text.strip().upper()

def process_data_iklan_harian(toko, file_order, file_iklan, file_seller, file_hourly=None):
    # Dictionary untuk menyimpan panjang maksimum setiap kolom
    col_widths = {}
    
    def update_width(col_idx, value):
        # Hitung panjang string valuenya
        width = len(str(value)) if value is not None else 0
        current_max = col_widths.get(col_idx, 0)
        if width > current_max:
            col_widths[col_idx] = width
            
    # 1. LOAD DATA
    df_order = pd.read_excel(file_order, dtype={'Total Harga Produk': str, 'Jumlah': str, 'Harga Satuan': str})
    df_iklan = pd.read_excel(file_iklan, skiprows=None)
    
    # Cek apakah file_seller ada isinya (Optional)
    if file_seller is not None:
        df_seller = pd.read_excel(file_seller, dtype={'Pengeluaran(Rp)': str})
    else:
        # Jika tidak ada, buat DataFrame kosong dengan kolom minimal agar tidak error saat merge
        df_seller = pd.DataFrame(columns=['Kode Pesanan', 'Pengeluaran(Rp)'])

    df_seller_export = df_seller.copy()

    df_hourly = None
    if file_hourly is not None:
        try:
            df_hourly = pd.read_excel(file_hourly, sheet_name='Hourly_Performance')
            # Bersihkan nama kolom
            df_hourly.columns = df_hourly.columns.str.strip()
            
            # Pastikan kolom yang dibutuhkan ada
            if 'Jam WIB' in df_hourly.columns and 'Lihat' in df_hourly.columns and 'Klik' in df_hourly.columns:
                # Konversi Jam WIB ke integer
                df_hourly['Jam WIB'] = pd.to_numeric(df_hourly['Jam WIB'], errors='coerce')
                df_hourly = df_hourly.dropna(subset=['Jam WIB'])
                df_hourly['Jam WIB'] = df_hourly['Jam WIB'].astype(int)
                
                # Konversi Lihat dan Klik ke numerik
                for col in ['Lihat', 'Klik']:
                    df_hourly[col] = pd.to_numeric(df_hourly[col], errors='coerce').fillna(0)
            else:
                st.warning("⚠️ Sheet 'Hourly_Performance' tidak memiliki kolom 'Jam WIB', 'Lihat', atau 'Klik'")
                df_hourly = None
        except Exception as e:
            st.warning(f"⚠️ Gagal membaca file hourly: {e}")
            df_hourly = None

    if df_hourly is not None:
        st.write("DEBUG df_hourly (3 baris pertama):")
        st.write(df_hourly[['Jam WIB', 'Lihat', 'Klik']].head(3))
    
        st.write("DEBUG tipe data df_hourly:")
        st.write(df_hourly[['Jam WIB', 'Lihat', 'Klik']].dtypes)

    # 2. PRE-PROCESS ORDER-ALL
    # Filter Status Pesanan != Batal dan Belum Bayar
    if 'Status Pesanan' in df_order.columns:
        status_filter = ['Batal', 'Belum Bayar']
        df_order = df_order[~df_order['Status Pesanan'].isin(status_filter)].copy()
    
    # Konversi kolom waktu
    if 'Waktu Pesanan Dibuat' in df_order.columns:
        df_order['Waktu Pesanan Dibuat'] = pd.to_datetime(df_order['Waktu Pesanan Dibuat'])
        df_order['Jam'] = df_order['Waktu Pesanan Dibuat'].dt.hour
        # Ambil tanggal untuk header laporan
        report_date = df_order['Waktu Pesanan Dibuat'].dt.strftime('%A, %d-%m-%Y').iloc[0] if not df_order.empty else "TANGGAL TIDAK DIKETAHUI"
    else:
        st.error("Kolom 'Waktu Pesanan Dibuat' tidak ditemukan di Order-all")
        return None

    df_order_export = df_order.copy()
    # Order-all
    for col in ['Total Harga Produk', 'Jumlah', 'Harga Satuan']:
        if col in df_order.columns:
            df_order[col] = (
                df_order[col]
                .astype(str)
                .str.replace('Rp', '', regex=False)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
            )
            df_order[col] = pd.to_numeric(df_order[col], errors='coerce').fillna(0)
    
    # Seller conversion
    if 'Pengeluaran(Rp)' in df_seller.columns:
        df_seller['Pengeluaran(Rp)'] = (
            df_seller['Pengeluaran(Rp)']
            .astype(str)
            .str.replace('Rp', '', regex=False)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
        )
        df_seller['Pengeluaran(Rp)'] = pd.to_numeric(df_seller['Pengeluaran(Rp)'], errors='coerce').fillna(0)

    # --- HITUNG EKSEMPLAR PER BARIS (GLOBAL) ---
    # 1. Bersihkan Variasi
    # df_order['Variasi_Clean'] = df_order['Nama Variasi'].apply(clean_variasi)
    # # 2. Hitung Total Eksemplar (Eksemplar per unit * Jumlah qty)
    # # Pastikan Jumlah sudah angka (float/int) dari proses clean sebelumnya
    # df_order['Eksemplar_Total'] = df_order.apply(
    #     lambda row: extract_eksemplar(row['Variasi_Clean']) * row['Jumlah'], axis=1
    # )
    # 1. Update variasi_clean dengan mengirimkan nama produk
    df_order['Variasi_Clean'] = df_order.apply(
        lambda x: clean_variasi(x['Nama Variasi'], x['Nama Produk']), axis=1
    )
    
    # 2. Update eksemplar dengan pengali 50 khusus paket wakaf
    def hitung_eksemplar_custom(row):
        base_eksemplar = extract_eksemplar(row['Variasi_Clean'])
        if "Paket Wakaf Murah 50 pcs Alquran Al Aqeel | Alquran 18 Baris" in str(row['Nama Produk']):
            return (base_eksemplar * 50) * row['Jumlah']
        return base_eksemplar * row['Jumlah']
    
    df_order['Eksemplar_Total'] = df_order.apply(hitung_eksemplar_custom, axis=1)

    # 3. PRE-PROCESS IKLAN (Sheet 'Iklan klik')
    df_iklan.columns = df_iklan.columns.str.strip()
    df_iklan_export = df_iklan.copy()
    
    # Bersihkan Nama Iklan
    if 'Nama Iklan' in df_iklan.columns:
        df_iklan['Nama Iklan'] = df_iklan['Nama Iklan'].apply(clean_nama_iklan)
        # Hapus Duplikat Nama Iklan
        df_iklan = df_iklan.drop_duplicates(subset=['Nama Iklan'])
    
    # Konversi kolom numerik di iklan
    cols_to_num = ['Dilihat', 'Jumlah Klik', 'Omzet Penjualan', 'Biaya']
    for col in cols_to_num:
        if col in df_iklan.columns:
            # Hapus simbol mata uang atau pemisah ribuan jika ada
            df_iklan[col] = df_iklan[col].astype(str).str.replace('Rp', '', regex=False).str.strip().str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df_iklan[col] = pd.to_numeric(df_iklan[col], errors='coerce').fillna(0)

    # 4. KATEGORISASI DATA (AFFILIATE, IKLAN, ORGANIK) & HIGHLIGHTING
    # Setup list untuk tracking
    list_affiliate_ids = df_seller['Kode Pesanan'].astype(str).tolist() if 'Kode Pesanan' in df_seller.columns else []
    list_iklan_names = df_iklan['Nama Iklan'].tolist() if 'Nama Iklan' in df_iklan.columns else []

    # Buat kolom helper di df_order
    df_order['is_affiliate'] = df_order['No. Pesanan'].astype(str).isin(list_affiliate_ids)
    df_order['is_iklan_product'] = df_order['Nama Produk'].apply(lambda x: clean_nama_iklan(x) in list_iklan_names)
    
    # Prioritas: Affiliate > Iklan (match product) > Organik
    # Namun prompt meminta: "Order all yg termasuk seller conversion (Affiliate)" dan "Diluar Seller Conversion dan Diluar Nama Iklan (Organik)"
    # Maka Sisanya (Diluar Seller tapi ADA di Nama Iklan) adalah Pesanan Iklan.
    
    df_affiliate = df_order[df_order['is_affiliate']].copy()
    df_organic = df_order[(~df_order['is_affiliate']) & (~df_order['is_iklan_product'])].copy()
    df_ads_orders = df_order[(~df_order['is_affiliate']) & (df_order['is_iklan_product'])].copy()

    # --- MEMBUAT DATA UNTUK LAPORAN ---

    # A. TABEL PESANAN IKLAN (Jam 0-23)
    # Prompt: "diambil dari order all dilihat di rentang jam... sum No Pesanan... Kuantitas... Omzet"
    # Menggunakan df_ads_orders (Pesanan yg berasal dari produk yg diiklanin)
    
    # --- AGGREGATION FUNCTIONS ---

    # A. TABEL PESANAN IKLAN (Fixed 24 Jam)
    hours_fixed = pd.DataFrame({'Jam': range(24)})
    
    # def agg_fixed_hours(df_source):
    #     if df_source.empty:
    #         return pd.DataFrame({'Jam': range(24), 'PESANAN': 0, 'KUANTITAS': 0, 'OMZET PENJUALAN': 0, 'JUMLAH EKSEMPLAR': 0})
    #     grp_pesanan = df_source.groupby('Jam')['No. Pesanan'].nunique().reset_index(name='PESANAN')
    #     grp_metrics = df_source.groupby('Jam')[['Jumlah', 'Total Harga Produk', 'Eksemplar_Total']].sum().reset_index()
    #     grp_metrics.rename(columns={'Jumlah': 'KUANTITAS', 'Total Harga Produk': 'OMZET PENJUALAN', 'Eksemplar_Total': 'JUMLAH EKSEMPLAR'}, inplace=True)
    #     merged = hours_fixed.merge(grp_pesanan, on='Jam', how='left').merge(grp_metrics, on='Jam', how='left')
    #     return merged.fillna(0)
    def agg_fixed_hours(df_source):
        if df_source.empty:
            # Tambahkan kolom LIHAT dan KLIK
            return pd.DataFrame({
                'Jam': range(24), 
                'PESANAN': 0, 
                'KUANTITAS': 0, 
                'OMZET PENJUALAN': 0, 
                'JUMLAH EKSEMPLAR': 0,
                'LIHAT': 0,
                'KLIK': 0
            })
        
        grp_pesanan = df_source.groupby('Jam')['No. Pesanan'].nunique().reset_index(name='PESANAN')
        grp_metrics = df_source.groupby('Jam')[['Jumlah', 'Total Harga Produk', 'Eksemplar_Total']].sum().reset_index()
        grp_metrics.rename(columns={'Jumlah': 'KUANTITAS', 'Total Harga Produk': 'OMZET PENJUALAN', 'Eksemplar_Total': 'JUMLAH EKSEMPLAR'}, inplace=True)
        
        merged = hours_fixed.merge(grp_pesanan, on='Jam', how='left').merge(grp_metrics, on='Jam', how='left')
        
        # ============================================================
        # TAMBAHAN: MERGE DENGAN DATA HOURLY (VIEWS & CLICKS)
        # ============================================================
        if df_hourly is not None and not df_hourly.empty:
            merged = merged.merge(
                df_hourly[['Jam WIB', 'Lihat', 'Klik']].rename(columns={'Jam WIB': 'Jam'}),
                on='Jam',
                how='left'
            )
            # Rename kolom ke uppercase untuk konsistensi
            merged = merged.rename(columns={'Lihat': 'LIHAT', 'Klik': 'KLIK'})
        else:
            merged['LIHAT'] = 0
            merged['KLIK'] = 0
            
        # Fill NaN dengan 0 untuk kolom LIHAT dan KLIK
        merged['LIHAT'] = merged['LIHAT'].fillna(0).astype(int)
        merged['KLIK'] = merged['KLIK'].fillna(0).astype(int)

        st.write("DEBUG setelah merge hourly (3 baris):")
        st.write(
            merged[['Jam', 'LIHAT', 'KLIK']]
            .head(3)
        )
        
        return merged.fillna(0)

    st.write("DEBUG Jam dari df_ads_orders:")
    st.write(df_ads_orders[['Jam']].drop_duplicates().sort_values('Jam').head(10))
    st.write("Tipe data Jam:", df_ads_orders['Jam'].dtype)
    
    tbl_iklan_data = agg_fixed_hours(df_ads_orders)

    # B. TABEL DINAMIS (AFFILIATE & ORGANIK)
    def agg_dynamic_hours(df_source, context=""):
        # PERBAIKAN: Definisikan kolom wajib agar tidak Error saat data kosong
        expected_cols = ['Jam', 'PESANAN', 'KUANTITAS', 'OMZET PENJUALAN', 'JUMLAH EKSEMPLAR']
        
        if df_source.empty:
            # Kembalikan DataFrame kosong TAPI dengan nama kolom yang sudah disiapkan
            return pd.DataFrame(columns=expected_cols) 
        
        grp_pesanan = df_source.groupby('Jam')['No. Pesanan'].nunique().reset_index(name='PESANAN')
        grp_metrics = df_source.groupby('Jam')[['Jumlah', 'Total Harga Produk', 'Eksemplar_Total']].sum().reset_index()
        grp_metrics.rename(columns={'Jumlah': 'KUANTITAS', 'Total Harga Produk': 'OMZET PENJUALAN', 'Eksemplar_Total': 'JUMLAH EKSEMPLAR'}, inplace=True)
        
        merged = grp_pesanan.merge(grp_metrics, on='Jam', how='left').fillna(0)
        merged = merged.sort_values('Jam')
        return merged
        
    # Note: Jika user ingin SEMUA pesanan masuk sini, ganti df_ads_orders dengan df_order. 
    # Tapi berdasarkan logika tabel Organik, harusnya ini dipisah. Saya gunakan df_ads_orders.

    # C. TABEL RINCIAN IKLAN KLIK
    total_dilihat = df_iklan['Dilihat'].sum()
    total_klik = df_iklan['Jumlah Klik'].sum()
    # Prompt: "Presentase Klik... dari 'Total Iklan Klik' dibagi 'Total Jumlah Klik'". 
    # Asumsi typo user: maksudnya Klik / Dilihat (CTR) atau Dilihat/Klik. 
    # Saya gunakan (Klik / Dilihat) * 100 karena %
    persentase_klik = (total_klik / total_dilihat) if total_dilihat > 0 else 0
    
    penjualan_iklan = tbl_iklan_data['OMZET PENJUALAN'].sum()

    # Hitung Biaya Iklan Spesifik
    # A5 Koran (Kapital di prompt berarti cek spesifik string atau case sensitive? Prompt: "tapi yang kapital")
    # Interpretasi: Mengandung "A5 Koran" DAN (Original String mengandung substring kapital atau case sensitive match)
    # Saya akan filter case sensitive untuk "A5 Koran" vs lower.
    
    # Helper filter contains
    def get_omzet_contains(query, case_sensitive=False):
        if case_sensitive:
            mask = df_iklan['Nama Iklan'].str.contains(query, case=True, regex=False)
        else:
            mask = df_iklan['Nama Iklan'].str.contains(query, case=False, regex=False)
        return df_iklan[mask]['Biaya'].sum()
        
    def get_biaya_regex(pattern, case_sensitive=False):
        if 'Biaya' not in df_iklan.columns:
            return 0
        # regex=True dan '.*' memungkinkan ada kata di tengah (misal: A5 Kertas Koran)
        mask = df_iklan['Nama Iklan'].str.contains(pattern, case=case_sensitive, regex=True, na=False)
        return df_iklan[mask]['Biaya'].sum()

    # --- LOGIKA BIAYA IKLAN PER TOKO ---
    rincian_biaya_khusus = [] # List tuple (Label, Value)

    if "Pacific Bookstore" in toko:
        # Pacific Logic
        # 1. A5 Kertas Koran
        b_a5_koran = get_biaya_regex(r"A5.*Kertas.*Koran", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan A5 Kertas Koran', b_a5_koran))
        
        # 2. A6 Kertas HVS
        b_a6_hvs = get_biaya_regex(r"Saku.*Pastel.*A6.*Kertas.*HVS", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan A6 Kertas HVS', b_a6_hvs))

        # 3. A6 Edisi Tahlil
        b_a6_tahlil = get_biaya_regex(r"Edisi.*Tahlilan.*A6.*Kertas.*HVS", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan A6 EDISI TAHLIL', b_a6_tahlil))

        biaya_gold = get_biaya_regex(r"Alquran.*GOLD.*Hard.*Cover", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan Al Aqeel Gold', biaya_gold))

        b_pkt_wakaf = get_biaya_regex(r"PAKET.*MURAH.*ALQURAN.*NON.*TERJEMAHAN", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan Paket Murah', b_pkt_wakaf))
        

    elif "DAMA.ID STORE" in toko:
        # DAMA.ID STORE Logic
        # # 1. A5 Kertas Koran
        b_a5_koran = get_biaya_regex(r"Alquran Al Aqeel A5 Kertas Koran Tanpa Terjemahan Wakaf Ibtida", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan A5 Koran', b_a5_koran))
        
        b_grosir = get_biaya_regex(r"Paket Hemat Paket Grosir Al Quran | AQ Al Aqeel Wakaf Kerta koran Non Terjemah", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan Paket Hemat Al Aqeel', b_grosir))
        
        # 3. A6 Edisi Tahlil
        b_a6_tahlil = get_biaya_regex(r"A6.*EDISI.*TAHLIL", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan A6 EDISI TAHLIL', b_a6_tahlil))

        biaya_gold = get_biaya_regex(r"Al.*Quran.*Gold.*Silver.*Aqeel", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan Al Aqeel Gold', biaya_gold))

        biaya_paket = get_biaya_regex(r"PAKET.*MURAH.*Alquran.*Al-Aqeel.*Tanpa.*Terjemahan.*BANDUNG.*Wakaf", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan Paket Al Aqeel Tanpa Terjemahan', biaya_paket))
        
    else:
        # HUMAN STORE (Default/Original Logic)
        # 1. A5 Koran (Kapital WAKAF)
        biaya_a5_koran = get_biaya_regex(r"AL QUR'AN NON TERJEMAH Al AQEEL A5 KERTAS KORAN WAKAF", case_sensitive=True)
        rincian_biaya_khusus.append(('Biaya Iklan A5 Koran', biaya_a5_koran))
        
        # 2. A6 Pastel
        biaya_a6_pastel = get_biaya_regex(r"AL QUR'AN A6 NON TERJEMAH HVS WARNA PASTEL", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan A6 Pastel', biaya_a6_pastel))
        
        # 3. A5 Koran Paket 7 (Sisa dari general A5 Koran dikurangi Kapital)
        total_a5_general = get_biaya_regex(r"Paket.*Alquran.*khusus.*A5.*Kertas.*Koran", case_sensitive=False)
        # biaya_a5_koran_pkt7 = total_a5_general - biaya_a5_koran
        biaya_a5_koran_pkt7 = total_a5_general
        rincian_biaya_khusus.append(('Biaya Iklan A5 Koran Paket 7', biaya_a5_koran_pkt7))
        
        # 4. Komik
        # biaya_komik = get_biaya_regex(r"Komik Pahlawan", case_sensitive=False)
        # rincian_biaya_khusus.append(('Biaya Iklan Komik Pahlawan', biaya_komik))
    
        # 4. Al Aqeel Gold (MENGGANTIKAN KOMIK PAHLAWAN)
        # Mendeteksi: "Alquran Cover Emas Kertas HVS Al Aqeel Gold Murah"
        # Kita pakai regex "Al Aqeel Gold" atau "Cover Emas" agar match
        biaya_gold = get_biaya_regex(r"Alquran Cover Emas Kertas HVS Al Aqeel A5 Gold Murah", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan A5 Gold', biaya_gold))

 
        biaya_gold_a7 = get_biaya_regex(r"Alquran Cover Emas Kertas HVS Al Aqeel A7 Gold Murah", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan A7 Gold', biaya_gold_a7))

        b_a6_tahlil = get_biaya_regex(r"AL QUR'AN EDISI TAHLILAN 30 Juz + Doa Tahlil | Pengganti Buku Yasin | Al Aqeel A6 Pastel HVS Edisi Tahlilan", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan A6 EDISI TAHLIL', b_a6_tahlil))

        biaya_pkt_wakaf = get_biaya_regex(r"Paket Wakaf Murah 50 pcs Alquran Al Aqeel | Alquran 18 Baris", case_sensitive=False)
        rincian_biaya_khusus.append(('Biaya Iklan Paket 50 pcs', biaya_pkt_wakaf))

    # Hitung Total Biaya Rinci
    total_biaya_iklan_rinci = sum([val for label, val in rincian_biaya_khusus])
    
    # Hitung ROASI
    roasi = (penjualan_iklan / total_biaya_iklan_rinci) if total_biaya_iklan_rinci > 0 else 0

    # SIAPKAN LIST ITEM UNTUK DITULIS KE EXCEL
    rincian_items = [
        ('Total Iklan Dilihat', total_dilihat),
        ('Total Jumlah Klik', total_klik),
        ('Presentase Klik', persentase_klik),
        ('Penjualan Iklan', penjualan_iklan),
    ]
    
    # Masukkan rincian biaya dinamis ke list
    for label, val in rincian_biaya_khusus:
        rincian_items.append((label, val))
        
    # Tambahkan ROASI di akhir
    rincian_items.append(('ROASI', roasi))
    
    # D. PREP AFFILIATE & ORGANIK DATA
    tbl_affiliate_data = agg_dynamic_hours(df_affiliate)
    # Tambah komisi untuk affiliate
    if not tbl_affiliate_data.empty:
        # if 'Kode Pesanan' in df_seller.columns and 'Pengeluaran(Rp)' in df_seller.columns:
        #     df_aff_merged = df_affiliate.merge(df_seller[['Kode Pesanan', 'Pengeluaran(Rp)']], left_on='No. Pesanan', right_on='Kode Pesanan', how='left')
        #     komisi_per_jam = df_aff_merged.groupby('Jam')['Pengeluaran(Rp)'].sum().reset_index()
        #     tbl_affiliate_data = tbl_affiliate_data.merge(komisi_per_jam, on='Jam', how='left').fillna(0)
        #     tbl_affiliate_data.rename(columns={'Pengeluaran(Rp)': 'KOMISI'}, inplace=True)
        # else:
        #     tbl_affiliate_data['KOMISI'] = 0
        if 'Kode Pesanan' in df_seller.columns and 'Pengeluaran(Rp)' in df_seller.columns:
        
            # Buat copy agar tidak merusak data export
            df_seller_calc = df_seller.copy()
            
            # LANGKAH KUNCI: Sum Komisi per Kode Pesanan DULU biar jadi 1 baris per pesanan
            # Jadi misal Order ID 123 ada 3 baris komisi, disatukan dulu totalnya.
            komisi_per_order = df_seller_calc.groupby('Kode Pesanan')['Pengeluaran(Rp)'].sum().reset_index()
            
            # 2. Siapkan Mapping Jam dari Order Affiliate
            # Kita butuh info: Order ID X itu jam berapa?
            # Ambil unik No. Pesanan dan Jam saja (Drop duplicate produk dalam 1 order)
            order_time_map = df_affiliate[['No. Pesanan', 'Jam']].drop_duplicates()
            
            # 3. Gabungkan Data Komisi Bersih dengan Jam
            # Merge: Order ID (yang ada Jam) + Total Komisi (dari Seller)
            merged_komisi = order_time_map.merge(
                komisi_per_order, 
                left_on='No. Pesanan', 
                right_on='Kode Pesanan', 
                how='inner' # Hanya ambil yang datanya ada di kedua file
            )
            
            # 4. Group by Jam lagi untuk masuk ke Tabel Laporan
            komisi_per_jam = merged_komisi.groupby('Jam')['Pengeluaran(Rp)'].sum().reset_index()
            komisi_per_jam.rename(columns={'Pengeluaran(Rp)': 'KOMISI'}, inplace=True)
            
            # 5. Masukkan ke Tabel Akhir
            tbl_affiliate_data = tbl_affiliate_data.merge(komisi_per_jam, on='Jam', how='left').fillna(0)
            
        else:
            # Jika file seller tidak ada atau kosong
            tbl_affiliate_data['KOMISI'] = 0
            
    tbl_organik_data = agg_dynamic_hours(df_organic)

    # E. TABEL RINCIAN SELURUH PESANAN (Product Level)
    # 1. Siapkan kolom variasi bersih
    # df_order['Variasi_Clean'] = df_order['Nama Variasi'].apply(clean_variasi)
    if 'Nama Variasi' in df_order.columns:
        df_order['Variasi_Clean'] = df_order.apply(
            lambda x: clean_variasi(x['Nama Variasi'], x['Nama Produk']), axis=1
        )
    else:
        df_order['Variasi_Clean'] = ''
    
    # 2. Group by Nama Produk & Variasi
    # Sum kolom 'Jumlah' untuk mendapatkan total qty produk tersebut
    grp_rincian = df_order.groupby(['Nama Produk', 'Variasi_Clean']).agg(
        Kuantitas=('Jumlah', 'sum') 
    ).reset_index()
    
    # 3. Hitung Eksemplar (Kuantitas * Eksemplar per variasi)
    # grp_rincian['Jumlah Eksemplar'] = grp_rincian.apply(
    #     lambda row: extract_eksemplar(row['Variasi_Clean']) * row['Kuantitas'], axis=1
    # )
    grp_rincian['Jumlah Eksemplar'] = grp_rincian.apply(
        lambda row: (
            extract_eksemplar(row['Variasi_Clean']) * 50 * row['Kuantitas']
            if "Paket Wakaf Murah 50 pcs Alquran Al Aqeel | Alquran 18 Baris" in str(row['Nama Produk'])
            else extract_eksemplar(row['Variasi_Clean']) * row['Kuantitas']
        ),
        axis=1
    )

    # F. TABEL SUMMARY
    total_omzet_all = 0
    
    # 1. Tambah Iklan
    if 'OMZET PENJUALAN' in tbl_iklan_data.columns:
        total_omzet_all += tbl_iklan_data['OMZET PENJUALAN'].sum()
        
    # 2. Tambah Affiliate (Cek empty dan kolom)
    if not tbl_affiliate_data.empty and 'OMZET PENJUALAN' in tbl_affiliate_data.columns:
        total_omzet_all += tbl_affiliate_data['OMZET PENJUALAN'].sum()
        
    # 3. Tambah Organik (Cek empty dan kolom)
    if not tbl_organik_data.empty and 'OMZET PENJUALAN' in tbl_organik_data.columns:
        total_omzet_all += tbl_organik_data['OMZET PENJUALAN'].sum()
    
    # Hitung Komisi
    total_komisi_aff = 0
    if not tbl_affiliate_data.empty and 'KOMISI' in tbl_affiliate_data.columns:
        total_komisi_aff = tbl_affiliate_data['KOMISI'].sum()
        
    # Hitung ROASF
    roasf = total_omzet_all / (total_biaya_iklan_rinci + total_komisi_aff) if (total_biaya_iklan_rinci + total_komisi_aff) > 0 else 0

    # --- MEMBUAT FILE EXCEL ---
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    workbook = writer.book
    
    # FORMATS
    fmt_header_main = workbook.add_format({'bold': True, 'font_size': 14, 'bg_color': '#ADD8E6', 'align': 'left', 'valign': 'vcenter'})
    fmt_header_table = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
    fmt_date = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
    fmt_col_name = workbook.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#f0f0f0'})
    fmt_num = workbook.add_format({'border': 1, 'align': 'center'})
    fmt_curr = workbook.add_format({'border': 1, 'num_format': '#,##0', 'align': 'center'})
    fmt_percent = workbook.add_format({'border': 1, 'num_format': '0.00%', 'align': 'center'})
    fmt_text_left = workbook.add_format({'border': 1, 'align': 'left'})

    # --- FORMAT WARNA HEADER BARU ---
    # Tabel 1: Orange (#FFA500)
    fmt_head_orange = workbook.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#FFA500'})
    # Tabel 2: Coklat (#D2691E - Chocolate, agar tulisan hitam masih terbaca)
    fmt_head_brown = workbook.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#D2691E', 'font_color': 'white'})
    # Tabel 3: Kuning (#FFFF00)
    fmt_head_yellow = workbook.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#FFFF00'})
    # Tabel 4: Pink (#FFC0CB)
    fmt_head_pink = workbook.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#FFC0CB'})
    # Tabel 5: Hijau (#90EE90)
    fmt_head_green = workbook.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#90EE90'})

    # --- FORMAT BOLD UNTUK SUMMARY ---
    fmt_text_left_bold = workbook.add_format({'border': 1, 'align': 'left', 'bold': True})
    fmt_curr_bold = workbook.add_format({'border': 1, 'num_format': '#,##0', 'align': 'center', 'bold': True})
    fmt_num_bold = workbook.add_format({'border': 1, 'align': 'center', 'bold': True})

    # TAMBAHKAN INI: Format angka dengan 2 desimal
    fmt_decimal = workbook.add_format({'border': 1, 'num_format': '0.00', 'align': 'center'})
    
    # --- SHEET 1: LAPORAN IKLAN ---
    ws_lap = workbook.add_worksheet('LAPORAN IKLAN')
    
    # Judul Utama
    ws_lap.merge_range('A1:S2', f'LAPORAN IKLAN SHOPEE {toko}', fmt_header_main)
    
    # --- TABEL 1: PESANAN IKLAN (A-F) ---
    start_row = 3 # Row 4
    ws_lap.merge_range(start_row, 0, start_row, 6, 'PESANAN IKLAN', fmt_head_orange)
    ws_lap.merge_range(start_row+1, 0, start_row+2, 6, report_date, fmt_date)
    
    cols_t1 = ['JAM', 'LIHAT', 'KLIK', 'PESANAN', 'KUANTITAS', 'OMZET PENJUALAN', 'JUMLAH EKSEMPLAR']
    for i, col in enumerate(cols_t1):
        ws_lap.write(start_row+3, i, col, fmt_col_name)
        update_width(i, col)

        
    # Isi Data Tabel 1
    row_cursor = start_row + 4
    for idx, row in tbl_iklan_data.iterrows():
        # Kolom Jam 00:00 - 23:00
        jam_str = f"{int(row['Jam']):02d}:00"
        ws_lap.write(row_cursor, 0, jam_str, fmt_num)
        update_width(0, jam_str)
        
        ws_lap.write(row_cursor, 1, row['LIHAT'], fmt_num)
        ws_lap.write(row_cursor, 2, row['KLIK'], fmt_num)
        ws_lap.write(row_cursor, 3, row['PESANAN'], fmt_num)
        ws_lap.write(row_cursor, 4, row['KUANTITAS'], fmt_num)
        ws_lap.write(row_cursor, 5, row['OMZET PENJUALAN'], fmt_curr)
        ws_lap.write(row_cursor, 6, row['JUMLAH EKSEMPLAR'], fmt_num)

        # Update widths
        update_width(1, row['LIHAT'])
        update_width(2, row['KLIK'])
        update_width(3, row['PESANAN'])
        update_width(4, row['KUANTITAS'])
        update_width(5, f"{row['OMZET PENJUALAN']:,}")
        
        row_cursor += 1
        
    # Total Tabel 1
    ws_lap.write(row_cursor, 0, "TOTAL", fmt_col_name)

    total_lihat = tbl_iklan_data['LIHAT'].sum()
    total_klik = tbl_iklan_data['KLIK'].sum()

    ws_lap.write(row_cursor, 1, total_lihat, fmt_col_name) # Total Lihat
    ws_lap.write(row_cursor, 2, total_klik, fmt_col_name)    # Total Klik
    ws_lap.write(row_cursor, 3, tbl_iklan_data['PESANAN'].sum(), fmt_col_name)
    ws_lap.write(row_cursor, 4, tbl_iklan_data['KUANTITAS'].sum(), fmt_col_name)
    ws_lap.write(row_cursor, 5, tbl_iklan_data['OMZET PENJUALAN'].sum(), fmt_col_name)
    ws_lap.write(row_cursor, 6, tbl_iklan_data['JUMLAH EKSEMPLAR'].sum(), fmt_col_name)
    update_width(5, f"{tbl_iklan_data['OMZET PENJUALAN'].sum():,}")
    
    # --- TABEL 2: RINCIAN IKLAN KLIK (G-H) ---
    t2_col_start = 8 # H
    t2_row = start_row
    
    ws_lap.merge_range(t2_row, t2_col_start, t2_row, t2_col_start+1, 'RINCIAN IKLAN KLIK', fmt_head_brown)
    
    curr_t2_row = t2_row + 1
    for label, val in rincian_items:
        # Tentukan format
        if 'Presentase' in label: fmt = fmt_percent
        elif 'ROAS' in label: fmt = fmt_decimal
        elif 'Total' in label and 'Dilihat' in label: fmt = fmt_num
        elif 'Total' in label and 'Klik' in label: fmt = fmt_num
        else: fmt = fmt_curr # Default currency
        
        ws_lap.write(curr_t2_row, t2_col_start, label, fmt_text_left)
        ws_lap.write(curr_t2_row, t2_col_start+1, val, fmt)
        
        update_width(t2_col_start, label)
        update_width(t2_col_start+1, str(val))
        curr_t2_row += 1

    # --- TABEL 3: PESANAN AFFILIATE (L-P) ---
    t3_col_start = 13 # M
    t3_row = start_row
    t3_cols = ['Jam', 'Pesanan', 'Kuantitas', 'Omzet Penjualan', 'Komisi', 'Jumlah Eksemplar']
    
    ws_lap.merge_range(t3_row, t3_col_start, t3_row, t3_col_start+5, 'PESANAN AFFILIATE', fmt_head_yellow)
    for i, col in enumerate(t3_cols):
        ws_lap.write(t3_row+1, t3_col_start+i, col, fmt_col_name)
        update_width(t3_col_start+i, col)
        
    curr_t3_row = t3_row + 2
    
    # Jika Data Kosong, buat 5 baris kosong
    if tbl_affiliate_data.empty:
        for _ in range(5):
             for i in range(6):
                 ws_lap.write(curr_t3_row, t3_col_start+i, "", fmt_num)
             curr_t3_row += 1
    else:
        for idx, row in tbl_affiliate_data.iterrows():
            ws_lap.write(curr_t3_row, t3_col_start, f"{int(row['Jam']):02d}:00", fmt_num)
            ws_lap.write(curr_t3_row, t3_col_start+1, row['PESANAN'], fmt_num)
            ws_lap.write(curr_t3_row, t3_col_start+2, row['KUANTITAS'], fmt_num)
            ws_lap.write(curr_t3_row, t3_col_start+3, row['OMZET PENJUALAN'], fmt_curr)
            ws_lap.write(curr_t3_row, t3_col_start+4, row['KOMISI'], fmt_curr)
            ws_lap.write(curr_t3_row, t3_col_start+5, row['JUMLAH EKSEMPLAR'], fmt_num)
            
            update_width(t3_col_start, f"{int(row['Jam']):02d}:00")
            update_width(t3_col_start+3, f"{row['OMZET PENJUALAN']:,}")
            curr_t3_row += 1

    # Total T3
    if not tbl_affiliate_data.empty:
        ws_lap.write(curr_t3_row, t3_col_start, "TOTAL", fmt_col_name)
        ws_lap.write(curr_t3_row, t3_col_start+1, tbl_affiliate_data['PESANAN'].sum(), fmt_col_name)
        ws_lap.write(curr_t3_row, t3_col_start+2, tbl_affiliate_data['KUANTITAS'].sum(), fmt_col_name)
        ws_lap.write(curr_t3_row, t3_col_start+3, tbl_affiliate_data['OMZET PENJUALAN'].sum(), fmt_col_name)
        ws_lap.write(curr_t3_row, t3_col_start+4, tbl_affiliate_data['KOMISI'].sum(), fmt_col_name),
        ws_lap.write(curr_t3_row, t3_col_start+5, tbl_affiliate_data['JUMLAH EKSEMPLAR'].sum(), fmt_col_name)
        total_omzet_aff = tbl_affiliate_data['OMZET PENJUALAN'].sum()
        total_komisi_aff_val = tbl_affiliate_data['KOMISI'].sum()
        roasa = total_omzet_aff / total_komisi_aff_val if total_komisi_aff_val > 0 else 0
        curr_t3_row += 1
        
        # ROASA
        ws_lap.write(curr_t3_row, t3_col_start, "ROASA", fmt_col_name)
        ws_lap.merge_range(curr_t3_row, t3_col_start+1, curr_t3_row, t3_col_start+2, "", fmt_num)
        ws_lap.write(curr_t3_row, t3_col_start+3, roasa, fmt_decimal)
        ws_lap.write(curr_t3_row, t3_col_start+4, "", fmt_num)
        curr_t3_row += 1
    
    last_row_affiliate = curr_t3_row

    # --- TABEL 4: PESANAN ORGANIK (M-P) ---
    t4_row = last_row_affiliate + 2
    t4_col_start = t3_col_start # M
    t4_cols = ['Jam', 'Pesanan', 'Kuantitas', 'Omzet Penjualan', 'Jumlah Eksemplar']
    
    ws_lap.merge_range(t4_row, t4_col_start, t4_row, t4_col_start+4, 'PESANAN ORGANIK', fmt_head_pink)
    for i, col in enumerate(t4_cols):
        ws_lap.write(t4_row+1, t4_col_start+i, col, fmt_col_name)
        update_width(t4_col_start+i, col)

    curr_t4_row = t4_row + 2
    if tbl_organik_data.empty:
        for _ in range(5):
             for i in range(5):
                 ws_lap.write(curr_t4_row, t4_col_start+i, "", fmt_num)
             curr_t4_row += 1
    else:
        for idx, row in tbl_organik_data.iterrows():
            ws_lap.write(curr_t4_row, t4_col_start, f"{int(row['Jam']):02d}:00", fmt_num)
            ws_lap.write(curr_t4_row, t4_col_start+1, row['PESANAN'], fmt_num)
            ws_lap.write(curr_t4_row, t4_col_start+2, row['KUANTITAS'], fmt_num)
            ws_lap.write(curr_t4_row, t4_col_start+3, row['OMZET PENJUALAN'], fmt_curr)
            ws_lap.write(curr_t4_row, t4_col_start+4, row['JUMLAH EKSEMPLAR'], fmt_num)
            update_width(t4_col_start, f"{int(row['Jam']):02d}:00")
            update_width(t4_col_start+3, f"{row['OMZET PENJUALAN']:,}")
            curr_t4_row += 1
            
    if not tbl_organik_data.empty:
        ws_lap.write(curr_t4_row, t4_col_start, "TOTAL", fmt_col_name)
        ws_lap.write(curr_t4_row, t4_col_start+1, tbl_organik_data['PESANAN'].sum(), fmt_col_name)
        ws_lap.write(curr_t4_row, t4_col_start+2, tbl_organik_data['KUANTITAS'].sum(), fmt_col_name)
        ws_lap.write(curr_t4_row, t4_col_start+3, tbl_organik_data['OMZET PENJUALAN'].sum(), fmt_col_name)
        ws_lap.write(curr_t4_row, t4_col_start+4, tbl_organik_data['JUMLAH EKSEMPLAR'].sum(), fmt_col_name)
        curr_t4_row += 1
        
    last_row_organik = curr_t4_row

    # --- TABEL 5: RINCIAN SELURUH PESANAN (H-K) ---
    t5_row = curr_t2_row + 2 
    t5_col_start = 8 # H
    
    total_seluruh_pesanan_val = tbl_iklan_data['PESANAN'].sum()
    if not tbl_affiliate_data.empty: total_seluruh_pesanan_val += tbl_affiliate_data['PESANAN'].sum()
    if not tbl_organik_data.empty: total_seluruh_pesanan_val += tbl_organik_data['PESANAN'].sum()
    
    ws_lap.write(t5_row, t5_col_start, 'RINCIAN SELURUH PESANAN', fmt_head_green)
    ws_lap.write(t5_row, t5_col_start+1, total_seluruh_pesanan_val, fmt_header_table)
    ws_lap.merge_range(t5_row, t5_col_start+2, t5_row, t5_col_start+3, "", fmt_header_table)
    
    t5_cols = ['Nama Produk', 'Variasi', 'Kuantitas', 'Jumlah Eksemplar']
    for i, col in enumerate(t5_cols):
        ws_lap.write(t5_row+1, t5_col_start+i, col, fmt_col_name)
        update_width(t5_col_start+i, col)
        
    curr_t5_row = t5_row + 2
    for idx, row in grp_rincian.iterrows():
        ws_lap.write(curr_t5_row, t5_col_start, row['Nama Produk'], fmt_text_left)
        ws_lap.write(curr_t5_row, t5_col_start+1, row['Variasi_Clean'], fmt_num)
        ws_lap.write(curr_t5_row, t5_col_start+2, row['Kuantitas'], fmt_num)
        ws_lap.write(curr_t5_row, t5_col_start+3, row['Jumlah Eksemplar'], fmt_num)
        
        update_width(t5_col_start, row['Nama Produk'])
        update_width(t5_col_start+1, row['Variasi_Clean'])
        curr_t5_row += 1
        
    ws_lap.write(curr_t5_row, t5_col_start+2, "TOTAL EKSEMPLAR", fmt_col_name)
    ws_lap.write(curr_t5_row, t5_col_start+3, grp_rincian['Jumlah Eksemplar'].sum(), fmt_col_name)
    update_width(t5_col_start+2, "TOTAL EKSEMPLAR")
    
    # --- TABEL 6: SUMMARY (M-Q) ---
    # Posisi: 2 baris spasi dibawah Organik
    t6_row = curr_t5_row + 2
    t6_col_start = 8 # H
    
    summary_data = [
        ('Penjualan Keseluruhan', total_omzet_all, fmt_curr),
        ('Total Biaya Iklan Klik', total_biaya_iklan_rinci, fmt_curr),
        ('Total Komisi Affiliate', total_komisi_aff, fmt_curr),
        ('ROASF', roasf, fmt_decimal)
    ]
    
    for label, val, fmt in summary_data:
        # Tentukan format nilai (Currency atau Number/Percent) tapi versi BOLD
        if fmt == fmt_curr:
            use_fmt = fmt_curr_bold
        else:
            use_fmt = fmt_num_bold # Default ke num bold untuk ROASF
            
        ws_lap.merge_range(t6_row, t6_col_start, t6_row, t6_col_start+1, label, fmt_text_left)
        ws_lap.write(t6_row, t6_col_start+2, val, fmt)
        update_width(t6_col_start, label)
        update_width(t6_col_start+2, str(val))
        t6_row += 1

    # --- APPLY AUTO WIDTH ---
    for col_idx, max_len in col_widths.items():
        # Set minimal width 10, max width 50, buffer +2 char
        width = max(10, min(max_len + 2, 50))
        ws_lap.set_column(col_idx, col_idx, width)

    # --- SIMPAN SHEET LAINNYA ---
    # 1. order-all (dengan highlight)
    df_order_export['is_affiliate'] = df_order['is_affiliate']
    df_order_export['is_iklan_product'] = df_order['is_iklan_product']
    
    df_order_export.to_excel(writer, sheet_name='order-all', index=False)
    ws_order = workbook.get_worksheet_by_name('order-all')
    
    # Format Highlight
    fmt_yellow = workbook.add_format({'bg_color': '#FFFF00'})
    fmt_pink = workbook.add_format({'bg_color': '#FFC0CB'})
    
    # Iterasi untuk highlight
    # Note: XlsxWriter tidak bisa overwrite format cell dengan mudah tanpa menulis ulang
    # Kita loop df_order untuk nulis ulang baris dengan format yang sesuai
    
    # Get columns for rewrite
    columns = df_order_export.columns.tolist()
    
    for row_idx, row_data in df_order_export.iterrows():
        row_fmt = None
        if row_data['is_affiliate']:
            row_fmt = fmt_yellow
        # Kondisi Pink: Diluar Affiliate (sudah checked via elif/logic) DAN Diluar Iklan
        # Logika Highlight Pink: "diluar yang termasuk Seller conversion dan diluar dari Nama Produk yang tidak ada di Nama Iklan"
        elif not row_data['is_iklan_product']:
            row_fmt = fmt_pink
            
        if row_fmt:
            for col_idx, col_name in enumerate(columns):
                # Tulis ulang cell dengan format. Handle datetime convert
                val = row_data[col_name]
                # Sederhanakan penulisan (XlsxWriter handle basic types)
                if pd.isna(val):
                    ws_order.write(row_idx + 1, col_idx, "", row_fmt)
                else:
                     # Check if datetime
                    if isinstance(val, pd.Timestamp):
                        ws_order.write_datetime(row_idx + 1, col_idx, val, workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm', 'bg_color': row_fmt.bg_color}))
                    else:
                        ws_order.write(row_idx + 1, col_idx, val, row_fmt)

    # 2. Iklan klik (Cleaned)
    df_iklan_export.to_excel(writer, sheet_name='Iklan klik', index=False)
    
    # 3. Seller conversion (Raw)
    df_seller_export.to_excel(writer, sheet_name='Seller conversion', index=False)

    writer.close()
    output.seek(0)
    return output, report_date


# ============================================
# UI UTAMA - STREAMLIT
# ============================================

def main():
    st.title("📊 Mysopipi (Rekapanku & Iklanku Shopee Only")
    st.markdown("Aplikasi untuk membuat Laporan Iklan Harian dan Rekapan Mingguan Shopee secara otomatis")
    st.markdown("---")

    # Pilihan Mode Utama
    mode = st.radio(
        "Pilih Mode:",
        ["Iklan Harian (1 hari)", "Rekapan Mingguan (7 hari)"],
        horizontal=True,
        key='mode_pilihan'
    )

    # Pilihan Toko
    store_choice = st.selectbox(
        "Pilih Toko:",
        ["Human Store", "Pacific Bookstore", "DAMA.ID STORE", "Raka Bookstore"],
        key='store_pilihan'
    )

    st.markdown("---")

    # UI berdasarkan mode
    if mode == "Iklan Harian (1 hari)":
        st.header("🎯 Mode: Laporan Iklan Harian")
        
        col1, col2 = st.columns(2)
        with col1:
            file_order = st.file_uploader("Upload 'Order-all' (xlsx)", type=['xlsx'], key='iklan_order')
            file_iklan = st.file_uploader("Upload 'Iklan Keseluruhan' (xlsx)", type=['xlsx'], key='iklan_iklan')
        with col2:
            file_seller = st.file_uploader("Upload 'Seller conversion' (xlsx) - Opsional", type=['xlsx'], key='iklan_seller')
            file_hourly = st.file_uploader("Upload 'Data Klik Views' (xlsx) - Hourly", type=['xlsx'], key='iklan_hourly')

        
        if st.button("🚀 Mulai Proses Iklan Harian", type="primary", key='btn_iklan'):
            if file_order and file_iklan:
                with st.spinner('Memproses data iklan harian...'):
                    try:
                        excel_file, report_date = process_data_iklan_harian(store_choice, file_order, file_iklan, file_seller, file_hourly)
                        suffix_date = report_date.replace('/', '_')
                        st.success("✅ Selesai!")
                        st.download_button(
                            label="📥 Download Laporan Iklan Harian",
                            data=excel_file,
                            file_name=f"LAPORAN_IKLAN_{store_choice.upper()}_{suffix_date}.xlsx",
                            key='dl_iklan'
                        )
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        st.exception(e)
            else:
                st.warning("⚠️ Harap upload file Order-all dan Iklan Keseluruhan!")

    else:  # Rekapan Mingguan
        st.header("📈 Mode: Rekapan Mingguan")
        
        # Load file katalog (wajib ada)
        try:
            katalog_df = pd.read_excel('HARGA ONLINE.xlsx')
            katalog_df.columns = [str(c).strip().upper() for c in katalog_df.columns]
            for col in ["JUDUL AL QUR'AN", "JENIS KERTAS", "UKURAN", "KATALOG HARGA"]:
                if col not in katalog_df.columns:
                    katalog_df[col] = ""
            katalog_df['JUDUL_NORM'] = katalog_df["JUDUL AL QUR'AN"].astype(str).str.upper().str.replace(r'[^A-Z0-9\s]', ' ', regex=True)
            katalog_df['JENIS_KERTAS_NORM'] = katalog_df['JENIS KERTAS'].astype(str).str.upper().str.replace(r'[^A-Z0-9\s]', ' ', regex=True)
            katalog_df['UKURAN_NORM'] = katalog_df['UKURAN'].astype(str).str.upper().str.replace(r'\s+', '', regex=True)
            katalog_df['KATALOG_HARGA_NUM'] = pd.to_numeric(katalog_df['KATALOG HARGA'].astype(str).str.replace(r'[^0-9\.]', '', regex=True), errors='coerce').fillna(0)
        except FileNotFoundError:
            st.error("❌ Error: File 'HARGA ONLINE.xlsx' tidak ditemukan.")
            return
        except Exception as e:
            st.error(f"❌ Error membaca HARGA ONLINE.xlsx: {e}")
            return

        try:
            harga_custom_tlj_df = pd.read_excel('Harga Custom TLJ.xlsx')
            harga_custom_tlj_df.columns = [str(c).strip().upper() for c in harga_custom_tlj_df.columns]
            required_cols = ['NAMA PRODUK', 'VARIASI', 'HARGA CUSTOM TLJ']
            if not all(col in harga_custom_tlj_df.columns for col in required_cols):
                st.error(f"File 'Harga Custom TLJ.xlsx' harus memiliki kolom: {', '.join(required_cols)}")
                return
            harga_custom_tlj_df['LOOKUP_KEY'] = harga_custom_tlj_df['NAMA PRODUK'].astype(str).str.strip() + ' ' + harga_custom_tlj_df['VARIASI'].astype(str).str.strip()
            harga_custom_tlj_df['HARGA CUSTOM TLJ'] = pd.to_numeric(harga_custom_tlj_df['HARGA CUSTOM TLJ'], errors='coerce').fillna(0)
        except FileNotFoundError:
            st.error("❌ Error: File 'Harga Custom TLJ.xlsx' tidak ditemukan.")
            return
        except Exception as e:
            st.error(f"❌ Error membaca Harga Custom TLJ.xlsx: {e}")
            return

        # Load KATALOG_DAMA jika toko adalah DAMA.ID STORE
        katalog_dama_df = None
        if store_choice == "DAMA.ID STORE":
            try:
                katalog_dama_df = pd.read_excel('KATALOG_DAMA.xlsx')
                katalog_dama_df.columns = [str(c).strip().upper() for c in katalog_dama_df.columns]
                required_dama_cols = ['NAMA PRODUK', 'JENIS AL QUR\'AN', 'WARNA', 'UKURAN', 'PAKET', 'HARGA']
                if not all(col in katalog_dama_df.columns for col in required_dama_cols):
                    st.error(f"File 'KATALOG_DAMA.xlsx' harus memiliki kolom: {', '.join(required_dama_cols)}")
                    return
                katalog_dama_df['HARGA'] = pd.to_numeric(katalog_dama_df['HARGA'], errors='coerce').fillna(0)
                for col in ['NAMA PRODUK', 'JENIS AL QUR\'AN', 'WARNA', 'UKURAN', 'PAKET']:
                    katalog_dama_df[col] = katalog_dama_df[col].fillna('').astype(str).str.strip().str.upper()
                    katalog_dama_df[col] = katalog_dama_df[col].str.replace(r'\s+', ' ', regex=True)
            except FileNotFoundError:
                st.error("❌ Error: File 'KATALOG_DAMA.xlsx' tidak ditemukan (wajib untuk DAMA.ID STORE).")
                return
            except Exception as e:
                st.error(f"❌ Error membaca KATALOG_DAMA.xlsx: {e}")
                return

        col1, col2 = st.columns(2)
        with col1:
            uploaded_order = st.file_uploader("1. Import file order-all.xlsx", type="xlsx", key='rekap_order')
            uploaded_income = st.file_uploader("2. Import file income dilepas.xlsx", type="xlsx", key='rekap_income')
        with col2:
            uploaded_iklan = st.file_uploader("3. Import file iklan produk (xlsx)", type="xlsx", key='rekap_iklan')
            uploaded_seller = st.file_uploader("4. Import file seller conversion (xlsx)", type="xlsx", key='rekap_seller')

        if st.button("🚀 Mulai Proses Rekapan Mingguan", type="primary", key='btn_rekap'):
            # Validasi file wajib
            base_files = uploaded_order and uploaded_income and uploaded_iklan
            
            if not base_files:
                st.warning("⚠️ Harap upload file Order-all, Income, dan Iklan!")
                return
                
            # Validasi seller conversion (wajib kecuali DAMA.ID STORE)
            if store_choice != "DAMA.ID STORE" and not uploaded_seller:
                st.warning(f"⚠️ Untuk toko {store_choice}, file Seller Conversion wajib diupload!")
                return

            with st.spinner('Memproses data rekapan mingguan...'):
                try:
                    # Baca data
                    order_all_df = pd.read_excel(uploaded_order, dtype={'Harga Setelah Diskon': str, 'Total Harga Produk': str})
                    income_dilepas_df = pd.read_excel(uploaded_income, sheet_name='Income', skiprows=None)
                    iklan_produk_df = pd.read_excel(uploaded_iklan, skiprows=None)
                    
                    if uploaded_seller:
                        seller_conversion_df = pd.read_excel(uploaded_seller)
                    else:
                        seller_conversion_df = pd.DataFrame(columns=['Kode Pesanan', 'Pengeluaran(Rp)'])

                    # Bersihkan data
                    cols_to_clean_order = ['Harga Setelah Diskon', 'Total Harga Produk']
                    for col in cols_to_clean_order:
                        if col in order_all_df.columns:
                            order_all_df[col] = clean_order_all_numeric(order_all_df[col])
    
                    other_financial_data_to_clean = [
                        (income_dilepas_df, ['Voucher dari Penjual', 'Biaya Administrasi', 'Biaya Proses Pesanan', 'Total Penghasilan']),
                        (iklan_produk_df, ['Biaya', 'Omzet Penjualan']),
                        (seller_conversion_df, ['Pengeluaran(Rp)'])
                    ]
    
                    for df, cols in other_financial_data_to_clean:
                        for col in cols:
                            if col in df.columns:
                                df[col] = clean_and_convert_to_numeric(df[col])

                    # Ambil tanggal
                    try:
                        df_date_raw = pd.read_excel(uploaded_income, sheet_name='Summary', header=None, nrows=10, usecols="B")
                        tgl_awal = df_date_raw.iloc[6, 0]
                        tgl_akhir = df_date_raw.iloc[7, 0]
                        date_range_str = get_pretty_date_range(tgl_awal, tgl_akhir)
                    except:
                        date_range_str = ""

                    # Proses berdasarkan toko
                    if store_choice in ["Human Store", "Raka Bookstore"]:
                        rekap_processed = process_rekap(order_all_df, income_dilepas_df, seller_conversion_df)
                        summary_processed = process_summary(rekap_processed, process_iklan(iklan_produk_df), katalog_df, harga_custom_tlj_df, store_type=store_choice)
                    elif store_choice == "Pacific Bookstore":
                        rekap_processed = process_rekap_pacific(order_all_df, income_dilepas_df, seller_conversion_df)
                        summary_processed = process_summary(rekap_processed, process_iklan(iklan_produk_df), katalog_df, harga_custom_tlj_df, store_type=store_choice)
                    elif store_choice == "DAMA.ID STORE":
                        rekap_processed = process_rekap_dama(order_all_df, income_dilepas_df, seller_conversion_df)
                        summary_processed = process_summary_dama(rekap_processed, process_iklan(iklan_produk_df), katalog_dama_df, harga_custom_tlj_df)
                    
                    iklan_processed = process_iklan(iklan_produk_df)

                    # Buat file output
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        workbook = writer.book
                        
                        title_format = workbook.add_format({'bold': True, 'fg_color': '#4472C4', 'font_color': 'white', 'align': 'left', 'valign': 'vcenter', 'font_size': 14})
                        header_format = workbook.add_format({'bold': True, 'fg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
                        cell_border_format = workbook.add_format({'border': 1})
                        percent_format = workbook.add_format({'num_format': '0.00%', 'border': 1})
                        one_decimal_format = workbook.add_format({'num_format': '0.0', 'border': 1})
                        total_fmt = workbook.add_format({'bold': True, 'fg_color': '#FFFF00', 'border': 1})
                        total_fmt_percent = workbook.add_format({'bold': True, 'fg_color': '#FFFF00', 'num_format': '0.00%', 'border': 1})
                        total_fmt_decimal = workbook.add_format({'bold': True, 'fg_color': '#FFFF00', 'num_format': '0.0', 'border': 1})

                        sheets = {
                            'SUMMARY': summary_processed, 
                            'REKAP': rekap_processed, 
                            'IKLAN': iklan_processed,
                            'sheet order-all': order_all_df, 
                            'sheet income dilepas': income_dilepas_df,
                            'sheet biaya iklan': iklan_produk_df, 
                            'sheet seller conversion': seller_conversion_df
                        }

                        for sheet_name, df in sheets.items():
                            start_row_data = 3 if sheet_name in ['SUMMARY', 'REKAP', 'IKLAN'] else 1
                            
                            df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row_data, header=False)
                            worksheet = writer.sheets[sheet_name]
                            
                            start_row_header = 0
                            if sheet_name in ['SUMMARY', 'REKAP', 'IKLAN']:
                                suffix_tgl = f" {date_range_str}" if date_range_str else ""
                                judul_sheet = f"{sheet_name} {store_choice.upper()} SHOPEE {suffix_tgl}"
                                worksheet.merge_range(0, 0, 1, len(df.columns) - 1, judul_sheet, title_format)
                                start_row_header = 2
                            
                            for col_num, value in enumerate(df.columns.values):
                                worksheet.write(start_row_header, col_num, value, header_format)

                            if sheet_name in ['SUMMARY', 'REKAP', 'IKLAN']:
                                worksheet.conditional_format(start_row_data, 0, start_row_data + len(df) - 1, len(df.columns) - 1, 
                                                             {'type': 'no_blanks', 'format': cell_border_format})

                            if sheet_name == 'SUMMARY':
                                persen_col = df.columns.get_loc('Persentase')
                                penjualan_hari_col = df.columns.get_loc('Penjualan Per Hari')
                                buku_pesanan_col = df.columns.get_loc('Jumlah buku per pesanan')
                                
                                for row_idx in range(len(df) - 1):
                                    excel_row = start_row_data + row_idx
                                    cell_value = df.iloc[row_idx, persen_col]
                                    worksheet.write(excel_row, persen_col, cell_value, percent_format)
                                
                                worksheet.set_column(persen_col, persen_col, 12)
                                worksheet.set_column(penjualan_hari_col, penjualan_hari_col, 18, one_decimal_format)
                                worksheet.set_column(buku_pesanan_col, buku_pesanan_col, 22, one_decimal_format)
                                
                                last_row = len(df) + start_row_header
                                for col_num in range(len(df.columns)):
                                    cell_value = df.iloc[-1, col_num]
                                    current_fmt = total_fmt
                                    if col_num == persen_col:
                                        current_fmt = total_fmt_percent
                                    elif col_num in [penjualan_hari_col, buku_pesanan_col]:
                                        current_fmt = total_fmt_decimal
                                    
                                    if pd.notna(cell_value):
                                        worksheet.write(last_row, col_num, cell_value, current_fmt)
                                    else:
                                        worksheet.write_blank(last_row, col_num, None, current_fmt)

                            if sheet_name == 'IKLAN':
                                last_row_idx = len(df) - 1
                                if not df.empty and df.iloc[last_row_idx]['Nama Iklan'] == 'TOTAL':
                                    for col_num in range(len(df.columns)):
                                        cell_value = df.iloc[last_row_idx, col_num]
                                        worksheet.write(start_row_data + last_row_idx, col_num, cell_value, total_fmt)
                            
                            for i, col in enumerate(df.columns):
                                column_len = max(df[col].astype(str).map(len).max(), len(col))
                                worksheet.set_column(i, i, column_len + 2)

                    output.seek(0)
                    
                    suffix_tgl = f" {date_range_str}" if date_range_str else ""
                    file_name_output = f"Rekapanku_Shopee_{store_choice}_{suffix_tgl}.xlsx"
                    
                    st.success("✅ Rekapan mingguan selesai!")
                    st.download_button(
                        label=f"📥 Download {file_name_output}",
                        data=output,
                        file_name=file_name_output,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key='dl_rekap'
                    )

                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    st.exception(e)


if __name__ == "__main__":
    main()
