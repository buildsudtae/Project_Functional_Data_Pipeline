## 📊 Instagram Usage & Lifestyle — Data Processing Pipeline

ระบบประมวลผลข้อมูลพฤติกรรมการใช้ Instagram และไลฟ์สไตล์ของผู้ใช้งาน พัฒนาด้วยหลักการ **Functional Programming** ในภาษา **Scala**

---

## ✨ ภาพรวม

ระบบรับไฟล์ CSV ต้นทาง แล้วผ่านกระบวนการแปลงค่า **3 ขั้นตอน (Step)** เพื่อสร้างคอลัมน์วิเคราะห์ใหม่ 3 คอลัมน์ จากนั้นส่งออกเป็นไฟล์ผลลัพธ์ 2 ชุด ได้แก่ **Sequential** และ **Concurrent**

| คอลัมน์ใหม่ | ความหมาย | ค่าที่เป็นไปได้ |
|-------------|----------|----------------|
| `ig_work_status` | สัดส่วนเวลา IG ต่อชั่วโมงงาน | Good / Normal / Bad / N/A |
| `bmi_health_status` | เกณฑ์ตาม BMI มาตรฐาน WHO | Underweight / Normal / Overweight / Obese |
| `digital_burnout_risk` | ความเสี่ยงต่อการ Burnout | High Risk / Medium Risk / Low Risk |

---

## 🗄️ Dataset

**ไฟล์:** `instagram_usage_lifestyle.csv`  
**ตำแหน่ง:** `src/main/resources/`

| คอลัมน์ | ประเภท | คำอธิบาย |
|---------|--------|----------|
| `user_id` | String | รหัสผู้ใช้งาน (Primary Key) |
| `age` | Int | อายุ (ปี) |
| `gender` | String | เพศ |
| `sleep_hours_per_night` | Double | จำนวนชั่วโมงนอนต่อคืน |
| `perceived_stress_score` | Double | คะแนนความเครียด (0–40) |
| `body_mass_index` | Double | ดัชนีมวลกาย (BMI) |
| `weekly_work_hours` | Double | ชั่วโมงทำงานต่อสัปดาห์ |
| `daily_active_minutes_instagram` | Double | นาทีที่ใช้ Instagram ต่อวัน |

---

## 🛠️ Technology Stack

| เทคโนโลยี / Library | Version | บทบาท |
|---------------------|---------|--------|
| Scala | 3.8.2 | ภาษาหลัก รองรับ FP + OOP |
| sbt | 1.12.5 | Build tool จัดการ dependency |
| `scala.util.Try` | Standard | FP error handling |
| `scala.util.Using` | Scala 2.13+ | Resource management ป้องกัน leak |
| `scala.concurrent.Future` | Standard | Asynchronous processing |
| `java.util.concurrent` | JDK 11+ | FixedThreadPool จัดการ Thread |
| `scala.io.Source` | Standard | Lazy file reading |
| `java.nio.file.Files` | JDK 7+ | Atomic file rename (.tmp → .csv) |

---

## 📁 โครงสร้างโปรเจกต์

```
Functional_Data_Pipeline/
├── build.sbt                          ← Scala version & dependencies
├── src/
│   └── main/
│       ├── resources/
│       │   └── instagram_usage_lifestyle.csv   ← ไฟล์ข้อมูลต้นทาง
│       └── scala/
│           └── Main.scala             ← โค้ดหลักทั้งหมด
└── data/                              ← Output (สร้างอัตโนมัติ)
    ├── instagram_seq.csv              ← ผลลัพธ์ Sequential Pipeline
    └── instagram_concur.csv           ← ผลลัพธ์ Concurrent Pipeline
```

`Main.scala` แบ่งออกเป็น 3 ส่วนหลัก:
- **Transformation Functions** — `step1Header/Data`, `step2Header/Data`, `step3Header/Data`
- **Processing Functions** — `processSequential`, `processConcurrent`
- **Main Entry Point** — `def main()` สร้าง Thread Pool และเรียก Pipeline ทั้งสองโหมด

---

## ⚙️ Business Logic ทั้ง 3 Step

### Step 1 — `ig_work_status`
เปรียบเทียบเวลา IG ต่อสัปดาห์กับชั่วโมงทำงาน (Ratio = IG hrs / Work hrs)

| สถานะ | เงื่อนไข | ความหมาย |
|-------|----------|----------|
| Good | ratio < 0.2 | ใช้ Social Media อยู่ในระดับสมดุล |
| Normal | 0.2 ≤ ratio ≤ 0.5 | อยู่ในเกณฑ์ยอมรับได้ |
| Bad | ratio > 0.5 | ใช้มากเกินไป ควรระวัง |
| N/A | workHrs = 0 | ไม่สามารถประเมินได้ |

### Step 2 — `bmi_health_status`
จัดกลุ่มตาม BMI มาตรฐาน WHO

| สถานะ | ช่วง BMI |
|-------|----------|
| Underweight | BMI < 18.5 |
| Normal | 18.5 ≤ BMI ≤ 24.9 |
| Overweight | 25.0 ≤ BMI ≤ 29.9 |
| Obese | BMI > 29.9 |

### Step 3 — `digital_burnout_risk`
ประเมินความเสี่ยง Digital Burnout จากความเครียดและชั่วโมงนอน

| สถานะ | เงื่อนไข |
|-------|----------|
| High Risk | stress ≥ 26.0 **AND** sleep ≤ 5.0 ชม. |
| Medium Risk | stress ≥ 14.0 **AND** sleep ≤ 6.0 ชม. |
| Low Risk | นอกเหนือจากเงื่อนไขข้างต้น |

---

## ⚡ ผลการเปรียบเทียบเวลาประมวลผล

> ทดสอบบนเครื่องที่มี **16 CPU Cores**

| | Step 1 | Step 2 | Step 3 | รวม |
|-|--------|--------|--------|-----|
| Sequential (1 Core) | 4,012 ms | 804 ms | 914 ms | **5,730 ms** |
| Concurrent (16 Cores) | 2,452 ms | 559 ms | 646 ms | **3,657 ms** |
| Speedup | 1.64× | 1.44× | 1.41× | **1.57×** |

> ค่าเวลาเป็นค่าประมาณ อาจแตกต่างตามสเปก CPU, จำนวน Core, ความเร็ว Storage และ JVM Warm-up

---

## 🚀 วิธีรันโปรเจกต์

### สิ่งที่ต้องติดตั้งก่อน
- [Scala 3](https://www.scala-lang.org/download/)
- [sbt](https://www.scala-sbt.org/download.html)

### ขั้นตอน

```bash
# 1. Clone repository
git clone <repository-url>
cd Functional_Data_Pipeline

# 2. รันโปรแกรม
sbt run
```

### ผลลัพธ์ที่ควรได้

```
Starting Pipeline with 16 Cores available...

--- Starting Sequential Processing (1 Core) ---
Step 1 (IG/Work Status): 4012 ms
Step 2 (BMI Status): 804 ms
Step 3 (Burnout Risk): 914 ms
>> Total Sequential Time: 5730 ms

--- Starting Concurrent Processing (16 Cores) ---
Step 1 (IG/Work Status): 2452 ms
Step 2 (BMI Status): 559 ms
Step 3 (Burnout Risk): 646 ms
>> Total Concurrent Time: 3657 ms

Pipeline Completed Successfully! Check output in the 'data' folder.
```

ไฟล์ output จะอยู่ที่ `data/instagram_seq.csv` และ `data/instagram_concur.csv`

```
user_id,age,gender,...,ig_work_status,bmi_health_status,digital_burnout_risk
1,51,Female,...,Good,Normal,Low Risk
2,64,Female,...,Bad,Normal,Low Risk
```

---

## 👥 ผู้จัดทำ

| ชื่อ - นามสกุล | รหัสนักศึกษา |
|----------------|-------------|
| นายอาวิษกรณ์ ตั้งประดิษฐ์ชัย | 67070200 |
| นายณัฏฐ์พงศ์ พรหมแก้ว | 67070227 |
| นายเปรม ศุภศรีสรรพ์ | 67070251 |
