"""
alarm_monitor.py
Konsument Kafka czytający z topicu 'alarms' (produkowanego przez Flink fraud detector).
Wyświetla alarmy w konsoli i generuje bogate wizualizacje wykrytych anomalii.

Format alarmu na topicu 'alarms':
{
  "card_id": "6B46B2F1A3D8E09C",
  "user_id": "USER_000042",
  "amount": 7.83,
  "timestamp": "2024-01-03T14:02:11",
  "gps": {"latitude": 52.245, "longitude": 21.033},
  "reasons": ["frequency", "speed"],
  "description": "scam likely - frequency, scam likely - speed",
  "previous_transaction": {
    "card_id": "6B46B2F1A3D8E09C",
    "user_id": "USER_000042",
    "timestamp": "2024-01-03T14:00:44",
    "gps": {"latitude": 48.85, "longitude": 2.35},
    "amount": 3.12,
    "computed": {
      "time_diff_sec": 87.0,
      "distance_m": 1275000.3,
      "speed_mps": 10119.05
    }
  },
  "current_stats": {"global_mean": 187.43, "global_std": 142.67}
}

Uruchomienie:
    python alarm_monitor.py
    python alarm_monitor.py --timeout 60
    python alarm_monitor.py --output-dir ./raporty
"""

import json
import argparse
import os
from datetime import datetime
from collections import defaultdict

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
from kafka import KafkaConsumer

# ============================================================================
# KONFIGURACJA
# ============================================================================

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'alarms'
GROUP_ID = 'alarm-monitor-group'

# ============================================================================
# KONSUMENT + ZBIERANIE DANYCH
# ============================================================================

def consume_alarms(timeout_sec=120):
    """Konsumuje alarmy z Kafki i zwraca listę."""

    print(f"[Kafka] Łączę z {KAFKA_BROKER}, topic '{TOPIC}'...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        # enable_auto_commit=True,
        # group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=timeout_sec * 1000,
    )

    alarms = []
    print(f"[Kafka] Konsumowanie alarmów (timeout: {timeout_sec}s)...\n")

    try:
        for message in consumer:
            alarm = message.value
            alarms.append(alarm)

            # Wyświetl alarm w konsoli
            n = len(alarms)
            desc = alarm.get('description', '?')
            card = alarm.get('card_id', '?')
            amt = alarm.get('amount', 0)
            ts = alarm.get('timestamp', '?')
            print(f"  [ALARM #{n:>4}] {ts} | karta {card} | {amt:>8.2f} PLN | {desc}")

            if n % 100 == 0:
                print(f"  --- zebrano {n} alarmów ---")

    except KeyboardInterrupt:
        print("\n[Stop] Przerwano ręcznie.")
    finally:
        consumer.close()

    print(f"\n[Kafka] Zakończono. Zebrano {len(alarms)} alarmów.")
    return alarms


# ============================================================================
# WIZUALIZACJA
# ============================================================================

def visualize(alarms, output_dir='./alarm_output'):
    """Generuje kompleksowe wykresy na podstawie zebranych alarmów."""

    if not alarms:
        print("[Vis] Brak alarmów do wizualizacji.")
        return

    os.makedirs(output_dir, exist_ok=True)

    # ----------- EKSTRAKCJA DANYCH -----------

    reasons_all = []           # każdy powód osobno
    reason_combos = []         # kombinacje powodów per alarm
    amounts = []
    hours = []
    days = []
    lats = []
    lons = []
    speeds = []                # prędkości z computed (tylko gdy jest)
    time_diffs = []            # różnice czasu [s]
    distances = []             # dystanse [m]
    users = []
    cards = []
    prev_lats = []
    prev_lons = []

    for a in alarms:
        reasons = a.get('reasons', [])
        for r in reasons:
            reasons_all.append(r)
        reason_combos.append(tuple(sorted(reasons)))

        amounts.append(a.get('amount', 0))
        users.append(a.get('user_id', ''))
        cards.append(a.get('card_id', ''))

        gps = a.get('gps', {})
        lats.append(gps.get('latitude', 0))
        lons.append(gps.get('longitude', 0))

        try:
            ts = datetime.fromisoformat(a['timestamp'])
            hours.append(ts.hour)
            days.append(ts.strftime('%Y-%m-%d'))
        except:
            pass

        prev = a.get('previous_transaction')
        if prev and prev.get('computed'):
            comp = prev['computed']
            speeds.append(comp.get('speed_mps', 0))
            time_diffs.append(comp.get('time_diff_sec', 0))
            distances.append(comp.get('distance_m', 0))
            prev_gps = prev.get('gps', {})
            prev_lats.append(prev_gps.get('latitude', 0))
            prev_lons.append(prev_gps.get('longitude', 0))

    # ----------- RAPORT GŁÓWNY (6x3 = 18 wykresów) -----------

    fig = plt.figure(figsize=(30, 42))
    gs = GridSpec(7, 3, figure=fig, hspace=0.4, wspace=0.3)
    fig.suptitle(f'ALARM MONITOR — Raport Anomalii\nZebrano {len(alarms)} alarmów',
                 fontsize=18, fontweight='bold', y=0.995)

    # --- 1. Rozkład typów anomalii (bar) ---
    ax = fig.add_subplot(gs[0, 0])
    reason_counts = defaultdict(int)
    for r in reasons_all:
        reason_counts[r] += 1
    labels = sorted(reason_counts.keys())
    values = [reason_counts[l] for l in labels]
    colors_map = {'frequency': '#e74c3c', 'speed': '#f39c12', 'night_hour': '#8e44ad', 'amount_outlier': '#2980b9'}
    bar_colors = [colors_map.get(l, '#95a5a6') for l in labels]
    bars = ax.bar(labels, values, color=bar_colors, edgecolor='white')
    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, str(v),
                ha='center', fontsize=10, fontweight='bold')
    ax.set_title('Liczba wykryć per typ anomalii', fontweight='bold')
    ax.set_ylabel('Liczba')
    ax.set_xlabel('Typ anomalii')

    # --- 2. Pie chart typów ---
    ax = fig.add_subplot(gs[0, 1])
    explode = [0.05] * len(labels)
    ax.pie(values, labels=labels, explode=explode, colors=bar_colors,
           autopct='%1.1f%%', shadow=True, startangle=90)
    ax.set_title('Proporcje typów anomalii', fontweight='bold')

    # --- 3. Kombinacje powodów (ile alarmów ma >1 powód) ---
    ax = fig.add_subplot(gs[0, 2])
    combo_counts = defaultdict(int)
    for combo in reason_combos:
        combo_counts[' + '.join(combo)] += 1
    sorted_combos = sorted(combo_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    combo_labels = [c[0] for c in sorted_combos]
    combo_vals = [c[1] for c in sorted_combos]
    ax.barh(combo_labels, combo_vals, color='#34495e', alpha=0.8)
    ax.set_title('Top 10 kombinacji anomalii', fontweight='bold')
    ax.set_xlabel('Liczba alarmów')
    ax.invert_yaxis()

    # --- 4. Rozkład kwot w alarmach ---
    ax = fig.add_subplot(gs[1, 0])
    ax.hist(amounts, bins=50, color='#e74c3c', alpha=0.7, edgecolor='white')
    ax.axvline(np.mean(amounts), color='black', linestyle='--',
               label=f'Średnia: {np.mean(amounts):.1f}')
    ax.axvline(np.median(amounts), color='blue', linestyle=':',
               label=f'Mediana: {np.median(amounts):.1f}')
    ax.set_title('Rozkład kwot w alarmach', fontweight='bold')
    ax.set_xlabel('Kwota [PLN]')
    ax.set_ylabel('Liczba')
    ax.legend()

    # --- 5. Kwoty per typ anomalii (boxplot) ---
    ax = fig.add_subplot(gs[1, 1])
    reason_amounts = defaultdict(list)
    for a in alarms:
        for r in a.get('reasons', []):
            reason_amounts[r].append(a.get('amount', 0))
    bp_labels = sorted(reason_amounts.keys())
    bp_data = [reason_amounts[l] for l in bp_labels]
    bp = ax.boxplot(bp_data, labels=bp_labels, patch_artist=True, showfliers=False)
    for patch, label in zip(bp['boxes'], bp_labels):
        patch.set_facecolor(colors_map.get(label, '#95a5a6'))
        patch.set_alpha(0.7)
    ax.set_title('Kwoty per typ anomalii', fontweight='bold')
    ax.set_ylabel('Kwota [PLN]')

    # --- 6. Rozkład godzinowy ---
    ax = fig.add_subplot(gs[1, 2])
    ax.hist(hours, bins=24, range=(0, 24), color='#8e44ad', alpha=0.7, edgecolor='white')
    ax.axvspan(1, 5, alpha=0.15, color='red', label='Strefa nocna (1-5)')
    ax.set_title('Alarmy wg godziny', fontweight='bold')
    ax.set_xlabel('Godzina')
    ax.set_ylabel('Liczba')
    ax.set_xticks(range(0, 24, 2))
    ax.legend()

    # --- 7. Dzienna liczba alarmów ---
    ax = fig.add_subplot(gs[2, 0:2])
    if days:
        day_counts = defaultdict(int)
        for d in days:
            day_counts[d] += 1
        sorted_days = sorted(day_counts.keys())
        day_vals = [day_counts[d] for d in sorted_days]
        ax.bar(range(len(sorted_days)), day_vals, color='#e74c3c', alpha=0.7)
        ax.set_title('Liczba alarmów dziennie', fontweight='bold')
        ax.set_xlabel('Dzień')
        ax.set_ylabel('Alarmy')
        step = max(1, len(sorted_days) // 10)
        ax.set_xticks(range(0, len(sorted_days), step))
        ax.set_xticklabels([sorted_days[i][5:] for i in range(0, len(sorted_days), step)], rotation=45)

    # --- 8. Skumulowane alarmy w czasie ---
    ax = fig.add_subplot(gs[2, 2])
    if days:
        cumulative = list(range(1, len(alarms) + 1))
        ax.plot(cumulative, color='#e74c3c', linewidth=1.5)
        ax.set_title('Alarmy skumulowanie', fontweight='bold')
        ax.set_xlabel('Nr alarmu')
        ax.set_ylabel('Suma')
        ax.grid(True, alpha=0.3)

    # --- 9. Mapa GPS alarmów ---
    ax = fig.add_subplot(gs[3, 0:2])
    ax.scatter(lons, lats, s=10, alpha=0.4, color='#e74c3c', label='Alarm (bieżąca TX)')
    if prev_lats:
        ax.scatter(prev_lons, prev_lats, s=10, alpha=0.3, color='#2980b9', marker='x',
                   label='Poprzednia TX')
        # Linie łączące pary (sample max 200 żeby nie zaśmiecić)
        sample_n = min(200, len(prev_lats))
        for i in range(sample_n):
            ax.plot([prev_lons[i], lons[i]], [prev_lats[i], lats[i]],
                    color='gray', alpha=0.1, linewidth=0.5)
    ax.set_title('Mapa lokalizacji alarmów (bieżąca vs poprzednia TX)', fontweight='bold')
    ax.set_xlabel('Długość geograficzna')
    ax.set_ylabel('Szerokość geograficzna')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # --- 10. Mapa — zoom Polska ---
    ax = fig.add_subplot(gs[3, 2])
    pl_lats = [la for la, lo in zip(lats, lons) if 49 <= la <= 55 and 14 <= lo <= 24.5]
    pl_lons = [lo for la, lo in zip(lats, lons) if 49 <= la <= 55 and 14 <= lo <= 24.5]
    ax.scatter(pl_lons, pl_lats, s=15, alpha=0.5, color='#e74c3c')
    ax.set_xlim(14, 24.5)
    ax.set_ylim(49, 55)
    ax.set_title('Alarmy — Polska', fontweight='bold')
    ax.set_xlabel('Długość')
    ax.set_ylabel('Szerokość')
    ax.grid(True, alpha=0.3)

    # --- 11. Rozkład prędkości (speed anomaly) ---
    ax = fig.add_subplot(gs[4, 0])
    if speeds:
        speed_arr = np.array(speeds)
        # Pokaż tylko te > 30 m/s (flagowane)
        flagged_speeds = speed_arr[speed_arr > 30]
        if len(flagged_speeds) > 0:
            ax.hist(flagged_speeds, bins=40, color='#f39c12', alpha=0.7, edgecolor='white')
            ax.axvline(30, color='red', linestyle='--', linewidth=2, label='Próg: 30 m/s')
            ax.axvline(np.median(flagged_speeds), color='black', linestyle=':',
                       label=f'Mediana: {np.median(flagged_speeds):.0f} m/s')
            ax.set_title('Rozkład prędkości (anomalie speed)', fontweight='bold')
            ax.set_xlabel('Prędkość [m/s]')
            ax.set_ylabel('Liczba')
            ax.legend()
        else:
            ax.text(0.5, 0.5, 'Brak anomalii speed', ha='center', va='center')
    else:
        ax.text(0.5, 0.5, 'Brak danych o prędkości', ha='center', va='center')
    ax.set_title('Prędkości wykryte jako anomalia', fontweight='bold')

    # --- 12. Rozkład różnic czasu (frequency anomaly) ---
    ax = fig.add_subplot(gs[4, 1])
    if time_diffs:
        freq_diffs = [t for t in time_diffs if 0 < t < 300]
        if freq_diffs:
            ax.hist(freq_diffs, bins=30, color='#e74c3c', alpha=0.7, edgecolor='white')
            ax.axvline(300, color='red', linestyle='--', linewidth=2, label='Próg: 300s')
            ax.axvline(np.mean(freq_diffs), color='black', linestyle=':',
                       label=f'Średnia: {np.mean(freq_diffs):.0f}s')
            ax.set_xlabel('Różnica czasu [s]')
            ax.set_ylabel('Liczba')
            ax.legend()
        else:
            ax.text(0.5, 0.5, 'Brak anomalii frequency', ha='center', va='center')
    else:
        ax.text(0.5, 0.5, 'Brak danych', ha='center', va='center')
    ax.set_title('Czas między TX (anomalie frequency)', fontweight='bold')

    # --- 13. Rozkład dystansów ---
    ax = fig.add_subplot(gs[4, 2])
    if distances:
        dist_km = [d / 1000 for d in distances if d > 0]
        if dist_km:
            ax.hist(dist_km, bins=40, color='#f39c12', alpha=0.7, edgecolor='white')
            ax.axvline(np.median(dist_km), color='black', linestyle=':',
                       label=f'Mediana: {np.median(dist_km):.0f} km')
            ax.set_xlabel('Dystans [km]')
            ax.set_ylabel('Liczba')
            ax.legend()
    ax.set_title('Dystans między kolejnymi TX (alarmy)', fontweight='bold')

    # --- 14. Top 15 najczęściej alarmowanych kart ---
    ax = fig.add_subplot(gs[5, 0])
    card_counts = defaultdict(int)
    for c in cards:
        card_counts[c] += 1
    top_cards = sorted(card_counts.items(), key=lambda x: x[1], reverse=True)[:15]
    if top_cards:
        tc_labels = [f"...{c[0][-6:]}" for c in top_cards]
        tc_vals = [c[1] for c in top_cards]
        ax.barh(tc_labels, tc_vals, color='#2c3e50', alpha=0.8)
        ax.set_title('Top 15 kart z alarmami', fontweight='bold')
        ax.set_xlabel('Liczba alarmów')
        ax.invert_yaxis()

    # --- 15. Top 15 najczęściej alarmowanych użytkowników ---
    ax = fig.add_subplot(gs[5, 1])
    user_counts = defaultdict(int)
    for u in users:
        user_counts[u] += 1
    top_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:15]
    if top_users:
        tu_labels = [u[0] for u in top_users]
        tu_vals = [u[1] for u in top_users]
        ax.barh(tu_labels, tu_vals, color='#8e44ad', alpha=0.7)
        ax.set_title('Top 15 użytkowników z alarmami', fontweight='bold')
        ax.set_xlabel('Liczba alarmów')
        ax.invert_yaxis()

    # --- 16. Ile alarmów per użytkownik (histogram) ---
    ax = fig.add_subplot(gs[5, 2])
    user_alarm_counts = list(user_counts.values())
    ax.hist(user_alarm_counts, bins=30, color='#8e44ad', alpha=0.7, edgecolor='white')
    ax.axvline(np.mean(user_alarm_counts), color='red', linestyle='--',
               label=f'Średnia: {np.mean(user_alarm_counts):.1f}')
    ax.set_title('Rozkład: alarmy per użytkownik', fontweight='bold')
    ax.set_xlabel('Liczba alarmów')
    ax.set_ylabel('Liczba użytkowników')
    ax.legend()

    # --- 17. Heatmapa: godzina vs typ anomalii ---
    ax = fig.add_subplot(gs[6, 0:2])
    reason_types = sorted(set(reasons_all))
    heatmap_data = np.zeros((len(reason_types), 24))
    for a in alarms:
        try:
            h = datetime.fromisoformat(a['timestamp']).hour
            for r in a.get('reasons', []):
                row = reason_types.index(r)
                heatmap_data[row, h] += 1
        except:
            pass
    im = ax.imshow(heatmap_data, aspect='auto', cmap='YlOrRd', interpolation='nearest')
    ax.set_yticks(range(len(reason_types)))
    ax.set_yticklabels(reason_types)
    ax.set_xticks(range(0, 24, 2))
    ax.set_xticklabels(range(0, 24, 2))
    ax.set_xlabel('Godzina')
    ax.set_title('Heatmapa: typ anomalii vs godzina', fontweight='bold')
    plt.colorbar(im, ax=ax, label='Liczba alarmów')

    # --- 18. Statystyki tekstowe ---
    ax = fig.add_subplot(gs[6, 2])
    ax.axis('off')

    unique_users = len(set(users))
    unique_cards = len(set(cards))
    multi_reason = sum(1 for combo in reason_combos if len(combo) > 1)

    stats_text = (
        f"{'═'*40}\n"
        f"  PODSUMOWANIE ALARMÓW\n"
        f"{'═'*40}\n\n"
        f"  Łączna liczba alarmów:  {len(alarms):>6,}\n"
        f"  Unikalne karty:         {unique_cards:>6,}\n"
        f"  Unikalni użytkownicy:   {unique_users:>6,}\n"
        f"  Alarmy z >1 powodem:    {multi_reason:>6,}\n\n"
        f"{'─'*40}\n"
        f"  KWOTY\n"
        f"  Średnia:       {np.mean(amounts):>10.2f} PLN\n"
        f"  Mediana:       {np.median(amounts):>10.2f} PLN\n"
        f"  Min:           {np.min(amounts):>10.2f} PLN\n"
        f"  Max:           {np.max(amounts):>10.2f} PLN\n\n"
        f"{'─'*40}\n"
        f"  PRĘDKOŚCI (anomalie speed)\n"
    )
    if speeds:
        flagged_sp = [s for s in speeds if s > 30]
        if flagged_sp:
            stats_text += (
                f"  Średnia:       {np.mean(flagged_sp):>10.1f} m/s\n"
                f"  Max:           {np.max(flagged_sp):>10.1f} m/s\n"
                f"  Liczba:        {len(flagged_sp):>10}\n"
            )
    else:
        stats_text += "  Brak danych\n"

    stats_text += (
        f"\n{'─'*40}\n"
        f"  TYPY ANOMALII\n"
    )
    for label in sorted(reason_counts.keys()):
        pct = reason_counts[label] / len(reasons_all) * 100
        stats_text += f"  {label:<18} {reason_counts[label]:>5} ({pct:.1f}%)\n"

    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=9,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9))

    # Zapis
    path = os.path.join(output_dir, 'alarm_report.png')
    plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f"[Vis] Raport główny zapisany: {path}")

    # ----------- RAPORT DODATKOWY: weryfikacja poprawności -----------
    generate_verification_report(alarms, output_dir)


def generate_verification_report(alarms, output_dir):
    """
    Raport weryfikacyjny — pokazuje dowody poprawności detekcji:
    - Czy card_id się zgadza między TX
    - Rozkład time_diff dla frequency
    - Rozkład speed dla speed
    - Godziny dla night_hour
    - Kwoty vs próg 3σ dla amount_outlier
    """

    fig, axes = plt.subplots(2, 3, figsize=(22, 14))
    fig.suptitle('Raport weryfikacji poprawności detekcji', fontsize=14, fontweight='bold')

    # 1. Weryfikacja card_id — czy current.card_id == previous.card_id
    ax = axes[0, 0]
    match_count = 0
    mismatch_count = 0
    no_prev_count = 0
    for a in alarms:
        prev = a.get('previous_transaction')
        if prev:
            if prev.get('card_id') == a.get('card_id'):
                match_count += 1
            else:
                mismatch_count += 1
        else:
            no_prev_count += 1

    ax.bar(['Zgodne\ncard_id', 'Niezgodne\ncard_id', 'Brak\npoprzedniej'],
           [match_count, mismatch_count, no_prev_count],
           color=['#27ae60', '#e74c3c', '#95a5a6'])
    ax.set_title('Weryfikacja: card_id bieżąca == poprzednia', fontweight='bold')
    ax.set_ylabel('Liczba alarmów')
    for i, v in enumerate([match_count, mismatch_count, no_prev_count]):
        ax.text(i, v + 1, str(v), ha='center', fontweight='bold')

    # 2. Frequency: time_diff < 300s
    ax = axes[0, 1]
    freq_alarms = [a for a in alarms if 'frequency' in a.get('reasons', [])]
    freq_diffs = []
    for a in freq_alarms:
        prev = a.get('previous_transaction')
        if prev and prev.get('computed'):
            freq_diffs.append(prev['computed'].get('time_diff_sec', 0))
    if freq_diffs:
        ax.hist(freq_diffs, bins=30, color='#e74c3c', alpha=0.7, edgecolor='white')
        ax.axvline(300, color='black', linewidth=2, linestyle='--', label='Próg 300s')
        violations = sum(1 for t in freq_diffs if t >= 300)
        ax.set_xlabel('Δt [sekundy]')
        ax.set_ylabel('Liczba')
        ax.set_title(f'Frequency: Δt < 300s\n(fałszywe powyżej progu: {violations})', fontweight='bold')
        ax.legend()
    else:
        ax.text(0.5, 0.5, 'Brak alarmów frequency', ha='center', va='center')
        ax.set_title('Frequency: Δt', fontweight='bold')

    # 3. Speed: speed > 30 m/s
    ax = axes[0, 2]
    speed_alarms = [a for a in alarms if 'speed' in a.get('reasons', [])]
    alarm_speeds = []
    for a in speed_alarms:
        prev = a.get('previous_transaction')
        if prev and prev.get('computed'):
            alarm_speeds.append(prev['computed'].get('speed_mps', 0))
    if alarm_speeds:
        ax.hist(alarm_speeds, bins=40, color='#f39c12', alpha=0.7, edgecolor='white')
        ax.axvline(30, color='black', linewidth=2, linestyle='--', label='Próg 30 m/s')
        violations = sum(1 for s in alarm_speeds if s <= 30)
        ax.set_xlabel('Prędkość [m/s]')
        ax.set_ylabel('Liczba')
        ax.set_title(f'Speed: v > 30 m/s\n(fałszywe poniżej progu: {violations})', fontweight='bold')
        ax.legend()
    else:
        ax.text(0.5, 0.5, 'Brak alarmów speed', ha='center', va='center')
        ax.set_title('Speed: prędkość', fontweight='bold')

    # 4. Night hour: godzina 1-5
    ax = axes[1, 0]
    night_alarms = [a for a in alarms if 'night_hour' in a.get('reasons', [])]
    night_hours = []
    for a in night_alarms:
        try:
            h = datetime.fromisoformat(a['timestamp']).hour
            night_hours.append(h)
        except:
            pass
    if night_hours:
        ax.hist(night_hours, bins=24, range=(0, 24), color='#8e44ad', alpha=0.7, edgecolor='white')
        ax.axvspan(1, 5, alpha=0.2, color='red', label='Strefa 1:00-5:00')
        violations = sum(1 for h in night_hours if h < 1 or h >= 5)
        ax.set_xlabel('Godzina')
        ax.set_ylabel('Liczba')
        ax.set_title(f'Night hour: h ∈ [1,5)\n(fałszywe poza strefą: {violations})', fontweight='bold')
        ax.set_xticks(range(0, 24, 2))
        ax.legend()
    else:
        ax.text(0.5, 0.5, 'Brak alarmów night_hour', ha='center', va='center')
        ax.set_title('Night hour', fontweight='bold')

    # 5. Amount outlier: kwota > mean + 3*std
    ax = axes[1, 1]
    outlier_alarms = [a for a in alarms if 'amount_outlier' in a.get('reasons', [])]
    outlier_amounts = []
    outlier_thresholds = []
    for a in outlier_alarms:
        outlier_amounts.append(a.get('amount', 0))
        stats = a.get('current_stats', {})
        mean = stats.get('global_mean', 0)
        std = stats.get('global_std', 0)
        outlier_thresholds.append(mean + 3 * std)
    if outlier_amounts:
        ax.scatter(range(len(outlier_amounts)), outlier_amounts, s=10, alpha=0.6,
                   color='#2980b9', label='Kwota alarmu')
        ax.scatter(range(len(outlier_thresholds)), outlier_thresholds, s=5, alpha=0.4,
                   color='red', label='Próg (μ+3σ)')
        violations = sum(1 for a, t in zip(outlier_amounts, outlier_thresholds) if a <= t)
        ax.set_xlabel('Nr alarmu')
        ax.set_ylabel('Kwota [PLN]')
        ax.set_title(f'Amount outlier: kwota > μ+3σ\n(fałszywe poniżej progu: {violations})', fontweight='bold')
        ax.legend()
    else:
        ax.text(0.5, 0.5, 'Brak alarmów amount_outlier', ha='center', va='center')
        ax.set_title('Amount outlier', fontweight='bold')

    # 6. Podsumowanie weryfikacji
    ax = axes[1, 2]
    ax.axis('off')
    total = len(alarms)
    summary = (
        f"{'═'*36}\n"
        f"  WERYFIKACJA POPRAWNOŚCI\n"
        f"{'═'*36}\n\n"
        f"  Łącznie alarmów:     {total}\n\n"
        f"  Card ID:\n"
        f"    Zgodne:            {match_count}\n"
        f"    Niezgodne:         {mismatch_count}\n"
        f"    Bez poprzedniej:   {no_prev_count}\n\n"
        f"  Frequency ({len(freq_alarms)} alarmów):\n"
        f"    Wszystkie Δt<300s: {'TAK ✓' if freq_diffs and all(t < 300 for t in freq_diffs) else 'NIE ✗' if freq_diffs else 'brak danych'}\n\n"
        f"  Speed ({len(speed_alarms)} alarmów):\n"
        f"    Wszystkie v>30m/s: {'TAK ✓' if alarm_speeds and all(s > 30 for s in alarm_speeds) else 'NIE ✗' if alarm_speeds else 'brak danych'}\n\n"
        f"  Night hour ({len(night_alarms)} alarmów):\n"
        f"    Wszystkie h∈[1,5): {'TAK ✓' if night_hours and all(1 <= h < 5 for h in night_hours) else 'NIE ✗' if night_hours else 'brak danych'}\n\n"
        f"  Amount outlier ({len(outlier_alarms)} alarmów):\n"
        f"    Wszystkie > próg:  {'TAK ✓' if outlier_amounts and all(a > t for a, t in zip(outlier_amounts, outlier_thresholds)) else 'NIE ✗' if outlier_amounts else 'brak danych'}\n"
    )
    ax.text(0.02, 0.98, summary, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle='round', facecolor='#eafaf1', alpha=0.9))

    plt.tight_layout()
    path = os.path.join(output_dir, 'alarm_verification.png')
    plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f"[Vis] Raport weryfikacji zapisany: {path}")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Monitor alarmów — konsument Kafka + wizualizacja')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Broker Kafka')
    parser.add_argument('--topic', default='alarms', help='Topic z alarmami')
    parser.add_argument('--group-id', default='alarm-monitor-group', help='Consumer group')
    parser.add_argument('--timeout', type=int, default=120, help='Timeout konsumpcji [s]')
    parser.add_argument('--output-dir', default='./alarm_output', help='Katalog na wykresy')
    args = parser.parse_args()

    KAFKA_BROKER = args.bootstrap_servers
    TOPIC = args.topic
    GROUP_ID = args.group_id

    alarms = consume_alarms(timeout_sec=args.timeout)
    visualize(alarms, output_dir=args.output_dir)