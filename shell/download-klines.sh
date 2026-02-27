#!/bin/bash

# This is a simple script to download klines by given parameters.
# Set this to where you want downloaded files to be stored.
OUTPUT_DIR="/home/vincent-4090/Documents/GitHub/binance-public-data/downloads"
DOWNLOAD_CHECKSUM=1
VERIFY_CHECKSUM=1

symbols=("BTCUSDT") # add symbols here to download
intervals=("1m" "1s")
years=("2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025")
months=(01 02 03 04 05 06 07 08 09 10 11 12)

baseurl="https://data.binance.vision/data/spot/monthly/klines"
mkdir -p "${OUTPUT_DIR}"

RUN_TS="$(date '+%Y%m%d_%H%M%S')"
CHECKSUM_LOG_FILE="${OUTPUT_DIR}/checksum-status-${RUN_TS}.log"

ok_count=0
failed_count=0
missing_checksum_count=0
not_verified_count=0
download_failed_count=0

log_checksum_status() {
  local status="$1"
  local zip_path="$2"
  local checksum_path="$3"
  printf "%s | %s | zip=%s | checksum=%s\n" \
    "$(date '+%Y-%m-%d %H:%M:%S')" "${status}" "${zip_path}" "${checksum_path}" >> "${CHECKSUM_LOG_FILE}"
}

echo "timestamp | status | zip | checksum" > "${CHECKSUM_LOG_FILE}"
echo "Checksum status log: ${CHECKSUM_LOG_FILE}"

if [[ "${VERIFY_CHECKSUM}" == "1" ]] && ! command -v sha256sum >/dev/null 2>&1; then
  echo "sha256sum not found. Disabling checksum verification."
  VERIFY_CHECKSUM=0
fi

for symbol in "${symbols[@]}"; do
  for interval in "${intervals[@]}"; do
    for year in "${years[@]}"; do
      for month in "${months[@]}"; do
        target_dir="${OUTPUT_DIR}/data/spot/monthly/klines/${symbol}/${interval}"
        mkdir -p "${target_dir}"
        url="${baseurl}/${symbol}/${interval}/${symbol}-${interval}-${year}-${month}.zip"
        file_name="${symbol}-${interval}-${year}-${month}.zip"
        zip_path="${target_dir}/${file_name}"
        checksum_file_name="${file_name}.CHECKSUM"
        checksum_path="${target_dir}/${checksum_file_name}"

        if wget -q -P "${target_dir}" "${url}"; then
          echo "downloaded: ${zip_path}"
        else
          rm -f "${zip_path}"
          echo "File not exist or download failed: ${url}"
          log_checksum_status "ZIP_DOWNLOAD_FAILED" "${zip_path}" "${checksum_path}"
          download_failed_count=$((download_failed_count + 1))
          continue
        fi

        if [[ "${DOWNLOAD_CHECKSUM}" == "1" ]]; then
          checksum_url="${url}.CHECKSUM"
          if wget -q -P "${target_dir}" "${checksum_url}"; then
            echo "downloaded checksum: ${checksum_path}"
          else
            rm -f "${checksum_path}"
            echo "Checksum file not found: ${checksum_url}"
            log_checksum_status "MISSING_CHECKSUM" "${zip_path}" "${checksum_path}"
            missing_checksum_count=$((missing_checksum_count + 1))
            continue
          fi

          if [[ "${VERIFY_CHECKSUM}" == "1" ]]; then
            if (cd "${target_dir}" && sha256sum -c "${checksum_file_name}" >/dev/null 2>&1); then
              echo "checksum ok: ${zip_path}"
              log_checksum_status "CHECKSUM_OK" "${zip_path}" "${checksum_path}"
              ok_count=$((ok_count + 1))
            else
              echo "checksum FAILED: ${zip_path}"
              log_checksum_status "CHECKSUM_FAILED" "${zip_path}" "${checksum_path}"
              failed_count=$((failed_count + 1))
            fi
          else
            log_checksum_status "CHECKSUM_NOT_VERIFIED" "${zip_path}" "${checksum_path}"
            not_verified_count=$((not_verified_count + 1))
          fi
        else
          log_checksum_status "CHECKSUM_DOWNLOAD_DISABLED" "${zip_path}" "${checksum_path}"
          not_verified_count=$((not_verified_count + 1))
        fi
      done
    done
  done
done

echo "Checksum summary: OK=${ok_count} FAILED=${failed_count} MISSING=${missing_checksum_count} NOT_VERIFIED=${not_verified_count} ZIP_DOWNLOAD_FAILED=${download_failed_count}"
echo "Checksum summary: OK=${ok_count} FAILED=${failed_count} MISSING=${missing_checksum_count} NOT_VERIFIED=${not_verified_count} ZIP_DOWNLOAD_FAILED=${download_failed_count}" >> "${CHECKSUM_LOG_FILE}"
