import scala.io.Source
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.concurrent._
import scala.concurrent.duration._
import java.util.concurrent.Executors
import scala.util.{Try, Success, Failure, Using}

object InstagramPipeline {

  // ===============
  // ฟังก์ชันการทำงานต่างๆ
  // ===============

  // กำหนดแม่แบบเป็น Array เพื่อที่จะกรอง Column ที่ไม่ต้องงการออกไป
  val requiredCols = Array(
    "user_id",
    "age",
    "gender",
    "sleep_hours_per_night",
    "perceived_stress_score",
    "body_mass_index",
    "weekly_work_hours",
    "daily_active_minutes_instagram"
  )

  // --- Step 1: กรองคอลัมน์ และ คำนวณสถานะการติด IG เทียบกับเวลางาน (ig_work_status) ---

  // ฟังก์ชันสร้าง Header ใหม่: เอาคอลัมน์ที่ต้องการมาต่อท้า่ยด้วย "ig_work_status"
  def step1Header(headers: Array[String]): Array[String] = {
    requiredCols :+ "ig_work_status"
  }

  // ฟังก์ชันการคำนวณสัดส่วนชั่วโมงการเล่น IG ต่อสัปดาห์กับชั่วโมงการทำงานในสัปดาห์
  // โดยการรับค่าจะรับเป็น hearder Array ของไฟล์ต้นฉบับ และ ข้อมูลในแต่ละ row ของข้อมูลต้นฉบับ
  // จากนั้นคืนค่าเป็น Array ข้อมูล Row ที่มีการแก้ไขและเพิ่มเติมข้อมูลเข้าไปแล้ว
  def step1Data(headers: Array[String], parts: Array[String]): Array[String] = {

    // ดึงเฉพาะข้อมูลของคอลัมน์ที่อยู่ใน requiredCols โดยการใช้ headers.indexOf(col) เพื่อหาตำแหน่งของข้อมูล Row ที่ต้องการ
    // ผ่านตำแหน่งของ requiredCols ที่ปรากฏใน header ต้นฉบับ
    val filteredParts = requiredCols.map(col => parts(headers.indexOf(col)))

    // หาตำแหน่ง Index ของคอลัมน์ที่ต้องใช้คำนวณ
    val igMinsIdx = headers.indexOf("daily_active_minutes_instagram")
    val workIdx = headers.indexOf("weekly_work_hours")

    // ป้องกันโปรแกรมพัง (Exception) ด้วย Try: 
    // ถ้าแปลงเป็นตัวเลขไม่ได้ (เช่น ข้อมูลแหว่ง เป็นค่าว่าง) จะใช้ .getOrElse(0.0) return 0.0 ให้แทน
    val igMins = Try(parts(igMinsIdx).toDouble).getOrElse(0.0)
    val workHrs = Try(parts(workIdx).toDouble).getOrElse(0.0)

    // แปลงนาทีเล่น IG ต่อวัน ให้เป็น ชั่วโมงต่อสัปดาห์
    val igWeeklyHrs = (igMins * 7) / 60.0

    // คำนวณสัดส่วน (Ratio) และประเมินสถานะ
    val status = if (workHrs > 0) {
      val ratio = igWeeklyHrs / workHrs
      if (ratio < 0.2) "Good"
      else if (ratio <= 0.5) "Normal"
      else "Bad"
    } else {
      "N/A" // ดักกรณีคนไม่ได้ทำงาน (workHrs = 0) ป้องกันการหารด้วยศูนย์ (Divide by Zero)
    }

    // แปะสถานะที่คำนวณได้ เข้าไปที่ท้ายสุดของ Array แล้วส่งคืนค่า
    filteredParts :+ status
  }


  // --- Step 2: เพิ่มคอลัมน์ประเมินสุขภาพจาก BMI (bmi_health_status) ---
  def step2Header(headers: Array[String]): Array[String] =
    headers :+ "bmi_health_status"

  // ฟังก์ชันการเพิ่มค่าข้อมูลแสดงสถานะว่าค่า BMI อยู่ช่วงไหน
  def step2Data(headers: Array[String], parts: Array[String]): Array[String] = {
    // ดึงค่า BMI อย่างปลอดภัยด้วย Try
    val bmi =
      Try(parts(headers.indexOf("body_mass_index")).toDouble).getOrElse(0.0)

    // จัดกลุ่มตามเกณฑ์ BMI มาตรฐาน
    val status =
      if (bmi <= 0) "Error"
      else if (bmi < 18.5) "Underweight"
      else if (bmi <= 24.9) "Normal"
      else if (bmi <= 29.9) "Overweight"
      else "Obese"

    parts :+ status
  }


  // --- Step 3: เพิ่มคอลัมน์ประเมินความเสี่ยงหมดไฟ (digital_burnout_risk) ---
  def step3Header(headers: Array[String]): Array[String] =
    headers :+ "digital_burnout_risk"
  
  // ฟังก์ชันเพิ่มค่าสถานะบอกความเสี่ยงต่อการ burnout โดยใช้ข้อมูล คะแนนความเครียด และ ชั่วโมงการนอน
  def step3Data(headers: Array[String], parts: Array[String]): Array[String] = {
    val stress = Try(parts(headers.indexOf("perceived_stress_score")).toDouble).getOrElse(0.0)
    val sleep = Try(parts(headers.indexOf("sleep_hours_per_night")).toDouble).getOrElse(0.0)

    // คำนวณความเสี่ยง: เครียดสูง + นอนน้อย = High Risk
    val risk =
      if (stress == 0 && sleep == 0) "Error"
      else if (stress >= 26.0 && sleep <= 5.0) "High Risk"
      else if (stress >= 14.0 && sleep <= 6.0) "Medium Risk"
      else "Low Risk"

    parts :+ risk
  }


  // ===============
  // การประมวลผลไฟล์
  // ===============

  // ฟังก์ชันประมวลผลแบบทำงานทีละบรรทัด (Sequential - ใช้ 1 Core)
  def processSequential(
      inputFile: String, // path ที่อยู่ไฟล์ต้นทาง
      outputFile: String, // path ที่อยู่ไฟล์ปลายทาง
      transformHeader: Array[String] => Array[String], // ฟังก์ชันสำหรับแก้ไขข้อมูล Column
      transformData: (Array[String], Array[String]) => Array[String]  // ฟังก์ชันสำหรับแก้ไขข้อมูลในแต่ละ Row
  ): Long = { // คืนค่าออกเป็น เวลา หน่วยเป็น millisec
    val start = System.nanoTime() // เริ่มตัวจับเวลาแบบ benchmark
    
    // สร้างไฟล์ชั่วคราว (.tmp) เพื่อเขียนข้อมูล ป้องกันการเขียนทับไฟล์จริงจนพังถ้าเกิด Error
    val tempFile = new File(outputFile + ".tmp")

    // Using.Manager: ตัวจัดการไฟล์ที่ต้องการติดต่อกับไฟล์ภายนอก
    // สั่งเปิดไฟล์อ่าน (source) และไฟล์เขียน (writer) โดยรับประกันว่าถ้าจบงานหรือพัง มันจะ .close() ให้ทันที
    val processResult = Using.Manager { use =>
      val source = use(Source.fromFile(inputFile))
      val writer = use(new PrintWriter(tempFile))

      // ดึงข้อมูลมาทีละบรรทัด
      val lines = source.getLines()
	  // เช็คไฟล์ข้อมูลว่าว่างไหม
      if (lines.hasNext) {
        // เป็นการข้อมูล Column ที่ได้จาก line และทำการ split เป็น Array โดยใช้ "," และ ถ้ามีข้อมูลที่ขาดไปจะเติมเป็น "" โดยใช้ -1
        val originalHeaders = lines.next().split(",", -1)
		// โยนเข้าฟังก์ชัน transformHeader จากนั้นทำการเปลี่ยนจาก Array เป็น String เพื่อที่จะเขียนลงไฟล์
        val newHeaders = transformHeader(originalHeaders).mkString(",")
        writer.println(newHeaders)

        // วนลูปอ่านข้อมูลทีละบรรทัด -> แปลงข้อมูล -> เขียนลงไฟล์ .tmp
        for (line <- lines) {
          val parts = line.split(",", -1)
		  // ส่งข้อมูล Column และ ข้อมูล row ที่ต้องการแก้เข้าไปในฟังก์ชัน transformData
          val newParts = transformData(originalHeaders, parts)
          writer.println(newParts.mkString(","))
        }
      }
    } // สิ้นสุดบล็อกนี้: ไฟล์ source และ writer ถูกปิดอย่างสมบูรณ์แบบ!

	//ตรวจสอบการทำงานของ processResult ที่ได้จากการใช้ Using.Manager
    processResult match {
      case Success(_) =>
        Try {
		  // วิธีการเปลี่ยนนามสกุลไฟล์จา่ก .csv.temp เป็น .csv
          Files.move( // Files มาจากการ import
            tempFile.toPath, // path ของ tempFile
            Paths.get(outputFile), // path และ ชื่อที่แท้จริง
            StandardCopyOption.REPLACE_EXISTING // เมื่อมีการยย้ายไปไฟล์ไปแล้วชื่อเหมือนจะทำการ Replace
          )
        } match {
          case Failure(e) =>
            println(s"[ERROR] Failed to move file: ${e.getMessage}")
          case Success(_) => // เสร็จสมบูรณ์
        }

      case Failure(e) =>
        // ถ้าพังระหว่างทาง: แจ้งเตือน Error และตามไปลบไฟล์ .tmp ทิ้งไม่ให้เป็นขยะรกเครื่อง
        println(s"[ERROR] Sequential failed: ${e.getMessage}")
        if (tempFile.exists()) tempFile.delete() // ลบ tempFile ที่ยังเขียนไม่เสร็จที่จะลดภาระ hard disk
    }

    // คืนค่าเวลาที่ใช้ไปในหน่วยมิลลิวินาที (ms)
    (System.nanoTime() - start) / 1_000_000
  }


  // ฟังก์ชันประมวลผลแบบขนาน (Concurrent - ใช้หลาย Cores พร้อมกัน)
  // รับค่า implicit ec (Thread Pool เพื่อใช้สำหรับการใช้ future และ cores แสดงจำนวน core
  def processConcurrent(
      inputFile: String, // path ที่อยู่ไฟล์ต้นทาง
      outputFile: String, // path ที่อยู่ไฟล์ปลายทาง
      transformHeader: Array[String] => Array[String], // ฟังก์ชันสำหรับแก้ไขข้อมูล Column
      transformData: (Array[String], Array[String]) => Array[String] // ฟังก์ชันสำหรับแก้ไขข้อมูลในแต่ละ Row
  )(implicit ec: ExecutionContext, cores: Int): Long = { // คืนค่าเป็นเวลา ในหน่อย millisec 
    val start = System.nanoTime() // เริ่มตัวจับเวลาแบบ benchmark
    // สร้างไฟล์ชั่วคราว (.tmp) เพื่อเขียนข้อมูล ป้องกันการเขียนทับไฟล์จริงจนพังถ้าเกิด Error
    val tempFile = new File(outputFile + ".tmp")

	// Using.Manager: ตัวจัดการไฟล์ที่ต้องการติดต่อกับไฟล์ภายนอก
    // สั่งเปิดไฟล์อ่าน (source) และไฟล์เขียน (writer) โดยรับประกันว่าถ้าจบงานหรือพัง มันจะ .close() ให้ทันที
    val processResult = Using.Manager { use =>
      val source = use(Source.fromFile(inputFile))
      val writer = use(new PrintWriter(tempFile))

      val lines = source.getLines() // ดึงข้อมูลมาทีละบรรทัด
      if (lines.hasNext) { // เช็ว่าไฟล์ว่างไหม
        val originalHeaders = lines.next().split(",", -1) 
        val newHeaders = transformHeader(originalHeaders).mkString(",")
        writer.println(newHeaders)
	  
        // การกำหนดจำนวนข้อมูลในการทำงานเพื่อไม่ให้ RAM เต็ม (อ่านข้อมูลทีละ 100,000 บรรทัด)
        val batchSize = 1000000
        
        // แบ่งข้อมูลออกเป็นกลุ่ม กลุ่มละ 100,000 บรรทัด
        lines.grouped(batchSize).foreach { batch =>
          
          // แบ่งข้อมูล 100,000 บรรทัดตามจำนวน cores ที่พร้อมใช้งานอีกที และจัดเป็น List
          val chunkSize =
            Math.max(1, Math.ceil(batch.size.toDouble / cores).toInt)
          val chunks = batch.grouped(chunkSize).toList

          // Parallel Execution: การประมวลผลพร้อมกันโดยการประกาศ Future และใช้ ec (Thread pool) จาก implicit ที่รับเข้ามา
          val futures = chunks.map { chunk =>
            Future {
              chunk.map { line =>
                val parts = line.split(",", -1) // เปลี่ยนข้อมูลเป็นลิส และ "" สำหรับข้อมูลที่ขาดหายไป
                transformData(originalHeaders, parts).mkString(",") // โยนเข้าฟังก์ชัน transformData และเปลี่ยนเปฌ็น string เพื่อให้พร้อมสำหรับเขียนลงไฟล์
              }
            } // (ec)
          }

          // Await & Flatten: รอทุก Process ทำงานเสร็จแบบไม่มีกำหนดเวลา แล้วนำมารวมเป็น Future[List] โดยใช้ .sequence จากนั้นยุบ List ซ่อน List โดยใช้ .flatten
          val processedBatch =
            Await.result(Future.sequence(futures), Duration.Inf).flatten
            
          // เขียน 1 แสนบรรทัดที่เสร็จแล้วลงไฟล์ .tmp แล้ววนไปดึง Batch ถัดไปมาทำ
          processedBatch.foreach(writer.println)
        }
      }
    } // สิ้นสุดบล็อก: ไฟล์ปิดสนิท

    // 2. จัดการผลลัพธ์แบบปลอดภัย (ลอจิกเดียวกับ Sequential)
    processResult match {
      case Success(_) =>
        Try {
          Files.move(
            tempFile.toPath,
            Paths.get(outputFile),
            StandardCopyOption.REPLACE_EXISTING
          )
        } match {
          case Failure(e) =>
            println(s"[ERROR] Failed to move file: ${e.getMessage}")
          case Success(_) => // เสร็จสมบูรณ์
        }

      case Failure(e) =>
        println(s"[ERROR] Concurrent failed: ${e.getMessage}")
        if (tempFile.exists()) tempFile.delete()
    }

    (System.nanoTime() - start) / 1_000_000
  }


  // ==========================================
  // 3. Main Execution (จุดเริ่มต้นโปรแกรม)
  // ==========================================
  def main(args: Array[String]): Unit = {
    // สร้างโฟลเดอร์ data ถ้ายังไม่มี
    val dataDir = new File("data")
    if (!dataDir.exists()) dataDir.mkdirs()

	// กำหนด path ต่างๆ
    val originalFile = "src/main/resources/instagram_usage_lifestyle.csv"
    val seqFile = "data/instagram_seq.csv"
    val concurFile = "data/instagram_concur.csv"

    // --- Bulkheading: สร้าง Thread Pool เฉพาะกิจป้องกันการกวนระบบอื่น ---
    val cores = Runtime.getRuntime.availableProcessors() // เช็คว่าคอมเครื่องนี้มีกี่ Core
    val executor = Executors.newFixedThreadPool(cores)   // สร้างตัวรันโปรแกรมตามจำนวน Core
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor) // แปะป้ายบอกให้ Future ใช้อันนี้
    implicit val c: Int = cores // จำนวน core

    println(s"Starting Pipeline with $cores Cores available...")

    // --- รันทดสอบแบบ Sequential (ทีละบรรทัด) ---
    // สังเกตว่า Output ไฟล์จาก Step 1 จะถูกส่งเป็น Input ให้ Step 2 (การทำ Pipeline Chain)
    println("\n--- Starting Sequential Processing (1 Core) ---")
    val s1 = processSequential(originalFile, seqFile, step1Header, step1Data)
    println(s"Step 1 (IG/Work Status): $s1 ms")

    val s2 = processSequential(seqFile, seqFile, step2Header, step2Data)
    println(s"Step 2 (BMI Status): $s2 ms")

    val s3 = processSequential(seqFile, seqFile, step3Header, step3Data)
    println(s"Step 3 (Burnout Risk): $s3 ms")

    println(s">> Total Sequential Time: ${s1 + s2 + s3} ms")

    // --- รันทดสอบแบบ Concurrent (รันพร้อมกันหลาย Cores) ---
    println(s"\n--- Starting Concurrent Processing ($cores Cores) ---")
    val c1 = processConcurrent(originalFile, concurFile, step1Header, step1Data)
    println(s"Step 1 (IG/Work Status): $c1 ms")

    val c2 = processConcurrent(concurFile, concurFile, step2Header, step2Data)
    println(s"Step 2 (BMI Status): $c2 ms")

    val c3 = processConcurrent(concurFile, concurFile, step3Header, step3Data)
    println(s"Step 3 (Burnout Risk): $c3 ms")

    println(s">> Total Concurrent Time: ${c1 + c2 + c3} ms")

    println(
      "\nPipeline Completed Successfully! Check output in the 'data' folder."
    )
    
    // ปิดการทำงาน
    executor.shutdown()
  }
}
