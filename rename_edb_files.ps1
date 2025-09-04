# PowerShell Script for KakaoTalk Database Restoration with Enhanced Migration Detection
# Version: 2.0 - Improved with enhanced error handling and safety features
# This script:
# 1. Detects confirmed migrations using ActionLogDB and immediate database failure patterns
# 2. Processes all directories recursively to find .edb files and their backup files
# 3. Creates secure backups of current .edb files (originalname.edb_YYYYMMDD_HHMMSS.backup.new)
# 4. Restores the FIRST backup file after migration timepoint (not latest)
# 5. Removes incompatible SQLite WAL/SHM files
# 6. Cleans up non-recoverable special databases and invalid keystore files
#
# IMPORTANT: Always backup your KakaoTalk data before running this script.
# This script performs destructive operations and should be used with caution.

# Initialize script with error handling
$ErrorActionPreference = 'Stop'

# Get current timestamp for renaming
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

# Configuration constants
$MIGRATION_WINDOW_DAYS = 3
$MIN_IMMEDIATE_BACKUPS = 2
$BACKUP_TIME_MARGIN_SECONDS = 3   # Allow immediate DB backups up to 3 seconds BEFORE ActionLogDB backup
$TIMEPOINT_REGEX = '^\d{8}_\d{6}$'
$BACKUP_EXTENSION = '.backup'
$EDB_EXTENSION = '.edb'
$WAL_EXTENSION = '.edb-wal'
$SHM_EXTENSION = '.edb-shm'

# Progress tracking for better user experience
function Write-Progress-Step {
    param(
        [string]$Step,
        [string]$Activity,
        [int]$PercentComplete = -1
    )
    
    if ($PercentComplete -ge 0) {
        Write-Progress -Activity $Step -Status $Activity -PercentComplete $PercentComplete
    }
    
    if ($VerbosePreference -eq 'Continue') {
        Write-Host "  [PROGRESS] $Activity" -ForegroundColor DarkCyan
    }
}

# Enhanced error handling function
function Write-ErrorDetails {
    param(
        [string]$Operation,
        [string]$Target,
        [System.Exception]$Exception
    )
    
    Write-Host "    ✗ Failed $Operation`: $Target" -ForegroundColor Red
    Write-Host "      Error: $($Exception.Message)" -ForegroundColor DarkRed
    if ($VerbosePreference -eq 'Continue') {
        Write-Host "      Stack: $($Exception.StackTrace)" -ForegroundColor DarkGray
    }
}

# Safe operation wrapper with WhatIf support
function Invoke-SafeOperation {
    param(
        [string]$Operation,
        [string]$Target,
        [scriptblock]$Action,
        [string]$SuccessMessage,
        [switch]$Critical = $false
    )
    
    if ($script:WhatIfMode) {
        Write-Host "    [PREVIEW] Would $Operation`: $Target" -ForegroundColor Cyan
        return $true
    }
    
    try {
        & $Action
        if ($SuccessMessage) {
            Write-Host "    ✓ $SuccessMessage" -ForegroundColor Green
        }
        return $true
    }
    catch {
        Write-ErrorDetails -Operation $Operation -Target $Target -Exception $_.Exception
        if ($Critical) {
            throw
        }
        return $false
    }
}

# Function to parse timepoint string to DateTime with validation
function Parse-TimepointToDateTime {
    param(
        [string]$timepoint,
        [string]$source = "timepoint"
    )
    
    if ($timepoint -notmatch $TIMEPOINT_REGEX) {
        throw "Invalid timepoint format: $timepoint (expected YYYYMMDD_HHMMSS)"
    }
    
    try {
        $dateStr = $timepoint.Substring(0, 8)
        $timeStr = $timepoint.Substring(9, 6)
        $year = [int]$dateStr.Substring(0, 4)
        $month = [int]$dateStr.Substring(4, 2)
        $day = [int]$dateStr.Substring(6, 2)
        $hour = [int]$timeStr.Substring(0, 2)
        $minute = [int]$timeStr.Substring(2, 2)
        $second = [int]$timeStr.Substring(4, 2)
        
        # Validate date components
        if ($year -lt 2000 -or $year -gt 2100) { throw "Invalid year: $year" }
        if ($month -lt 1 -or $month -gt 12) { throw "Invalid month: $month" }
        if ($day -lt 1 -or $day -gt 31) { throw "Invalid day: $day" }
        if ($hour -lt 0 -or $hour -gt 23) { throw "Invalid hour: $hour" }
        if ($minute -lt 0 -or $minute -gt 59) { throw "Invalid minute: $minute" }
        if ($second -lt 0 -or $second -gt 59) { throw "Invalid second: $second" }
        
        $result = New-Object DateTime $year, $month, $day, $hour, $minute, $second
        
        # Validate date is not in the future
        if ($result.Ticks -gt (Get-Date).AddDays(1).Ticks) {
            throw "Date cannot be in the future: $($result.ToString('yyyy-MM-dd HH:mm:ss'))"
        }
        
        return $result
    }
    catch {
        throw "Failed to parse $source timepoint '$timepoint': $_"
    }
}

# Function to validate file paths are within safe boundaries
function Test-SafePath {
    param(
        [string]$path,
        [string]$baseDirectory = (Get-Location).Path
    )
    
    try {
        $resolvedPath = Resolve-Path $path -ErrorAction SilentlyContinue
        if (-not $resolvedPath) { return $false }
        
        $normalizedPath = $resolvedPath.Path.TrimEnd('\').ToLower()
        $normalizedBase = $baseDirectory.TrimEnd('\').ToLower()
        
        return $normalizedPath.StartsWith($normalizedBase)
    }
    catch {
        return $false
    }
}

# Function to get safe parent directory with validation
function Get-SafeParentPath {
    param([string]$currentPath = (Get-Location).Path)
    
    # Check if we're at a root directory
    if ($currentPath.Length -le 3 -or $currentPath -match '^[A-Za-z]:\\?$') {
        throw "Cannot run from root directory. Please run from a subdirectory."
    }
    
    $parentPath = Split-Path $currentPath -Parent
    if (-not $parentPath -or -not (Test-Path $parentPath)) {
        throw "Unable to access parent directory from: $currentPath"
    }
    
    return $parentPath
}

Write-Host "KakaoTalk Database Restoration Tool v2.0" -ForegroundColor Cyan
Write-Host ("=" * 50) -ForegroundColor Cyan
Write-Host ""

# Step 1: Find and analyze ActionLogDB backup files to determine migration timepoint
Write-Host "Step 1: Analyzing migration timepoints..." -ForegroundColor Yellow
Write-Host ""

# Look for ActionLogDB backup files in parent directory ONLY
try {
    $parentPath = Get-SafeParentPath
    Write-Host "  Searching for ActionLogDB backups in: $parentPath" -ForegroundColor Gray
} catch {
    Write-Host "  ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

$actionLogBackups = Get-ChildItem -Path $parentPath -Filter "ActionLogDB.edb_*$BACKUP_EXTENSION" -ErrorAction SilentlyContinue | 
    Where-Object { $_.Name -match "ActionLogDB\.edb_(\d{8}_\d{6})\.backup$" -and (Test-SafePath $_.FullName $parentPath) } |
    Sort-Object Name -Descending

if ($actionLogBackups.Count -eq 0) {
    Write-Host "  ERROR: No ActionLogDB backup files found in parent directory." -ForegroundColor Red
    Write-Host "  Path checked: $parentPath" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  This indicates one of the following:" -ForegroundColor Yellow
    Write-Host "    1. No migration has occurred (no backup needed)" -ForegroundColor Yellow
    Write-Host "    2. You're in the wrong directory" -ForegroundColor Yellow
    Write-Host "    3. ActionLogDB backups have been deleted" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  If you're certain a migration occurred, you can manually specify a timepoint." -ForegroundColor Cyan
    Write-Host ""
    
    # Ask if user wants to continue with manual input
    Write-Host "Do you want to manually enter a timepoint? (Y/N): " -NoNewline -ForegroundColor Cyan
    $continueManual = Read-Host
    
    if ($continueManual -ne 'Y' -and $continueManual -ne 'y') {
        Write-Host "  Operation cancelled. No migration backup found." -ForegroundColor Red
        exit
    }
    
    Write-Host ""
    Write-Host "Please enter the migration timepoint in YYYYMMDD_HHMMSS format" -ForegroundColor Cyan
    Write-Host "Example: 20250115_143022" -ForegroundColor Gray
    $selectedTimepoint = Read-Host "Timepoint"
    
    # Validate format and reasonableness of manual input
    if ($selectedTimepoint -notmatch $TIMEPOINT_REGEX) {
        Write-Host "  ERROR: Invalid format. Expected YYYYMMDD_HHMMSS (e.g., 20250115_143022)" -ForegroundColor Red
        exit 1
    }
    
    # Additional validation for reasonableness
    try {
        $testDate = Parse-TimepointToDateTime $selectedTimepoint "manual input"
        Write-Host "  Validated manual timepoint: $($testDate.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor Green
    } catch {
        Write-Host "  ERROR: Invalid timepoint: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "  Found $($actionLogBackups.Count) ActionLogDB backup file(s):" -ForegroundColor Green
    Write-Host ""
    
    # Define immediate databases that open right after login
    # NOTE: talk_user_prf.edb and emoticon.edb are included for migration detection
    # but are deleted later as they are non-recoverable special cases
    $immediateDatabases = @(
        "talk_user_prf.edb",              # User profile (non-recoverable)
        "Contacts/mpi_v2.edb",            # Contacts database
        "MultiProfileDB.edb",             # Multi-profile database
        "chat_data/url_image_v2.edb",     # URL image cache
        "chat_data/talkfile.edb",         # File database
        "chat_data/talkmedia.edb",        # Media database
        "emoticon.edb",                   # Emoticons (non-recoverable)
        "chat_data/chatListInfo.edb",     # Chat list information
        "chat_data/chatLinkInfo.edb"      # Chat link information
    )
    
    # Analyze each ActionLogDB backup to find confirmed migrations
    $confirmedMigrations = @()
    foreach ($backup in $actionLogBackups) {
        if ($backup.Name -match "ActionLogDB\.edb_(\d{8}_\d{6})\.backup$") {
            $actionLogTimepoint = $matches[1]
            
            try {
                # Parse ActionLogDB timepoint to DateTime
                $actionLogDate = Parse-TimepointToDateTime $actionLogTimepoint "ActionLogDB"
                $windowEnd = $actionLogDate.AddDays($MIGRATION_WINDOW_DAYS)
                
                Write-Host "  Analyzing ActionLogDB backup: $actionLogTimepoint" -ForegroundColor Gray
                
                # Pre-cache all backup files for performance (avoid recursive searches in loops)
                if (-not $script:backupFileCache) {
                    Write-Host "    Building backup file cache..." -ForegroundColor DarkGray
                    $script:backupFileCache = @{}
                    Get-ChildItem -Path . -Filter "*$BACKUP_EXTENSION" -Recurse -ErrorAction SilentlyContinue | ForEach-Object {
                        if ($_.Name -match "^(.+)\.edb_(\d{8}_\d{6})\.backup$" -and (Test-SafePath $_.FullName)) {
                            $baseName = $matches[1]
                            $timepoint = $matches[2]
                            $key = "$($_.Directory.FullName)\$baseName"
                            
                            if (-not $script:backupFileCache.ContainsKey($key)) {
                                $script:backupFileCache[$key] = @()
                            }
                            $script:backupFileCache[$key] += @{
                                File = $_
                                Timepoint = $timepoint
                                DateTime = $null  # Parse lazily when needed
                            }
                        }
                    }
                    Write-Host "    Cache built: $($script:backupFileCache.Keys.Count) database groups found" -ForegroundColor DarkGray
                }
                
                # Use cached backup files for immediate DB analysis (much faster than recursive search)
                $immediateBackupsInWindow = @()
                foreach ($immediateDb in $immediateDatabases) {
                    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($immediateDb)
                    
                    # Look for this database in all cached locations
                    $matchingCacheKeys = $script:backupFileCache.Keys | Where-Object { $_ -match "\\$([regex]::Escape($baseName))$" }
                    
                    foreach ($cacheKey in $matchingCacheKeys) {
                        $immediateDbBackups = $script:backupFileCache[$cacheKey]
                        
                        foreach ($cachedBackup in $immediateDbBackups) {
                            try {
                                # Parse timepoint lazily if not already cached
                                if ($cachedBackup.DateTime -eq $null) {
                                    $cachedBackup.DateTime = Parse-TimepointToDateTime $cachedBackup.Timepoint "immediate database backup"
                                }
                                $backupDate = $cachedBackup.DateTime
                                
                                # Calculate time window with margin: allow backups up to 3 seconds BEFORE ActionLogDB
                                $windowStart = $actionLogDate.AddSeconds(-$BACKUP_TIME_MARGIN_SECONDS)
                                
                                # Check if backup is within margin window (from 3 seconds BEFORE to 3 days AFTER ActionLogDB)
                                if ($backupDate.Ticks -ge $windowStart.Ticks -and $backupDate.Ticks -le $windowEnd.Ticks) {
                                    $immediateBackupsInWindow += @{
                                        Database = $immediateDb
                                        Timepoint = $cachedBackup.Timepoint
                                        DateTime = $backupDate
                                        File = $cachedBackup.File
                                    }
                                }
                            }
                            catch {
                                Write-Host "    Warning: Could not parse timepoint $($cachedBackup.Timepoint)" -ForegroundColor DarkYellow
                            }
                        }
                    }
                }
                
                # Check if we have enough immediate backups to confirm migration
                if ($immediateBackupsInWindow.Count -ge $MIN_IMMEDIATE_BACKUPS) {
                    Write-Host "    ✓ Migration confirmed: Found $($immediateBackupsInWindow.Count) immediate DB backups within time window" -ForegroundColor Green
                    
                    # Find earliest immediate backup as migration reference
                    # Filter out any entries with null DateTime before sorting
                    $validBackups = $immediateBackupsInWindow | Where-Object { $_.DateTime -ne $null }
                    
                    if ($validBackups.Count -eq 0) {
                        Write-Host "    ✗ No valid immediate backups with valid DateTime" -ForegroundColor Red
                        continue
                    }
                    
                    # Sort by DateTime ticks to ensure correct chronological order
                    $sortedBackups = $validBackups | Sort-Object { $_.DateTime.Ticks }
                    
                    Write-Host "    All immediate backups in window (sorted):" -ForegroundColor DarkGray
                    $sortedBackups | ForEach-Object {
                        Write-Host "      $($_.Database): $($_.Timepoint)" -ForegroundColor DarkGray
                    }
                    
                    $earliestImmediate = $sortedBackups | Select-Object -First 1
                    $migrationReference = $earliestImmediate.Timepoint
                    
                    Write-Host "    Migration reference: $migrationReference (from $($earliestImmediate.Database))" -ForegroundColor Cyan
                    
                    # Add to confirmed migrations list
                    $confirmedMigrations += @{
                        ActionLogTimepoint = $actionLogTimepoint
                        MigrationReference = $migrationReference
                        ImmediateCount = $immediateBackupsInWindow.Count
                        EarliestDatabase = $earliestImmediate.Database
                        ActionLogDate = $actionLogDate
                        MigrationDate = $earliestImmediate.DateTime
                    }
                } else {
                    Write-Host "    ✗ Insufficient immediate backups ($($immediateBackupsInWindow.Count)) to confirm migration" -ForegroundColor DarkRed
                }
            }
            catch {
                Write-Host "    Warning: Could not parse ActionLogDB timepoint $actionLogTimepoint" -ForegroundColor DarkYellow
            }
        }
    }
    
    Write-Host ""
    
    # Check if we found any confirmed migrations
    if ($confirmedMigrations.Count -eq 0) {
        Write-Host "  ERROR: No confirmed migrations found." -ForegroundColor Red
        Write-Host "  This indicates either:" -ForegroundColor Yellow
        Write-Host "    1. No actual migration occurred (just ActionLogDB backup without immediate DB failures)" -ForegroundColor Yellow
        Write-Host "    2. Immediate database backups have been deleted" -ForegroundColor Yellow
        Write-Host "    3. Migration occurred but with fewer than 2 immediate DB failures" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "  If you're certain a migration occurred, you can manually specify a timepoint." -ForegroundColor Cyan
        Write-Host ""
        
        # Ask if user wants to continue with manual input
        Write-Host "Do you want to manually enter a migration reference timepoint? (Y/N): " -NoNewline -ForegroundColor Cyan
        $continueManual = Read-Host
        
        if ($continueManual -ne 'Y' -and $continueManual -ne 'y') {
            Write-Host "  Operation cancelled. No confirmed migration found." -ForegroundColor Red
            exit
        }
        
        Write-Host ""
        Write-Host "Please enter the migration reference timepoint in YYYYMMDD_HHMMSS format" -ForegroundColor Cyan
        Write-Host "Example: 20250115_143022" -ForegroundColor Gray
        $selectedTimepoint = Read-Host "Migration Reference Timepoint"
        
        # Validate format and reasonableness of manual input
        if ($selectedTimepoint -notmatch $TIMEPOINT_REGEX) {
            Write-Host "  ERROR: Invalid format. Expected YYYYMMDD_HHMMSS (e.g., 20250115_143022)" -ForegroundColor Red
            exit 1
        }
        
        # Additional validation for reasonableness
        try {
            $testDate = Parse-TimepointToDateTime $selectedTimepoint "manual input"
            Write-Host "  Validated manual timepoint: $($testDate.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor Green
        } catch {
            Write-Host "  ERROR: Invalid timepoint: $($_.Exception.Message)" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "  Found $($confirmedMigrations.Count) confirmed migration(s):" -ForegroundColor Green
        Write-Host ""
        
        # Display confirmed migrations
        for ($i = 0; $i -lt $confirmedMigrations.Count; $i++) {
            $migration = $confirmedMigrations[$i]
            
            # Format dates for display
            $actionLogFormatted = $migration.ActionLogDate.ToString("yyyy-MM-dd HH:mm:ss")
            $migrationFormatted = $migration.MigrationDate.ToString("yyyy-MM-dd HH:mm:ss")
            
            Write-Host "    [$($i + 1)] Migration Reference: $($migration.MigrationReference)" -ForegroundColor Cyan
            Write-Host "        ActionLogDB backup: $($migration.ActionLogTimepoint) ($actionLogFormatted)" -ForegroundColor Gray
            Write-Host "        Migration reference: $($migration.MigrationReference) ($migrationFormatted)" -ForegroundColor Gray
            Write-Host "        Confirmed by: $($migration.ImmediateCount) immediate DB backups (earliest: $($migration.EarliestDatabase))" -ForegroundColor Gray
            Write-Host ""
        }
        
        Write-Host "These represent confirmed migrations with accurate reference timepoints." -ForegroundColor Yellow
        Write-Host "Choose the migration you want to restore to:" -ForegroundColor Cyan
        Write-Host ""
        
        # Prompt user to select a migration
        $selection = Read-Host "Enter the number [1-$($confirmedMigrations.Count)] or the full migration reference (YYYYMMDD_HHMMSS)"
        
        # Check if user entered a number or a timepoint
        if ($selection -match '^\d+$') {
            $index = [int]$selection - 1
            if ($index -ge 0 -and $index -lt $confirmedMigrations.Count) {
                $selectedTimepoint = $confirmedMigrations[$index].MigrationReference
            } else {
                Write-Host "  Error: Invalid selection. Please choose a number between 1 and $($confirmedMigrations.Count)" -ForegroundColor Red
                exit
            }
        } elseif ($selection -match $TIMEPOINT_REGEX) {
            $matchedMigration = $confirmedMigrations | Where-Object { $_.MigrationReference -eq $selection }
            if ($matchedMigration) {
                $selectedTimepoint = $selection
            } else {
                Write-Host "  Warning: Entered timepoint not found in confirmed migrations list." -ForegroundColor Yellow
                Write-Host "  Using custom timepoint: $selection" -ForegroundColor Yellow
                $selectedTimepoint = $selection
            }
        } else {
            Write-Host "  Error: Invalid input format" -ForegroundColor Red
            exit
        }
    }
}

Write-Host ""
Write-Host "Selected restoration timepoint: " -NoNewline
Write-Host $selectedTimepoint -ForegroundColor Green

# Cache the parsed migration date for reuse (avoid redundant parsing)
try {
    $script:migrationDate = Parse-TimepointToDateTime $selectedTimepoint "migration reference"
    Write-Host "Migration date: $($script:migrationDate.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor Gray
} catch {
    Write-Host "  ERROR: Invalid migration timepoint: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Ask for confirmation with preview option
Write-Host "This will restore databases to timepoint $selectedTimepoint" -ForegroundColor Yellow
Write-Host ""
Write-Host "Choose action:" -ForegroundColor Cyan
Write-Host "  [Y] Execute - Perform actual database restoration" -ForegroundColor Green
Write-Host "  [P] Preview - Show what would be done without making changes" -ForegroundColor Yellow
Write-Host "  [N] Cancel - Exit without doing anything" -ForegroundColor Red
Write-Host ""
Write-Host "Your choice (Y/P/N): " -NoNewline -ForegroundColor Cyan
$confirmation = Read-Host

# Set WhatIf mode based on user choice
$script:WhatIfMode = $false
if ($confirmation -eq 'P' -or $confirmation -eq 'p') {
    $script:WhatIfMode = $true
    Write-Host ""
    Write-Host "PREVIEW MODE: No actual changes will be made" -ForegroundColor Yellow
    Write-Host ""
} elseif ($confirmation -ne 'Y' -and $confirmation -ne 'y') {
    Write-Host "  Operation cancelled by user." -ForegroundColor Red
    exit
}

# Kill KakaoTalk.exe process before database operations (only in execute mode)
if (-not $script:WhatIfMode) {
    Write-Host ""
    Write-Host "Terminating KakaoTalk.exe process..." -ForegroundColor Yellow
    
    $kakaoProcesses = Get-Process -Name "KakaoTalk" -ErrorAction SilentlyContinue
    
    if ($kakaoProcesses) {
        try {
            $kakaoProcesses | Stop-Process -Force -ErrorAction Stop
            Write-Host "  ✓ KakaoTalk.exe process terminated successfully" -ForegroundColor Green
            # Give a brief moment for process to fully terminate
            Start-Sleep -Milliseconds 500
        }
        catch {
            Write-Host "  ✗ Failed to terminate KakaoTalk.exe: $($_.Exception.Message)" -ForegroundColor Red
            Write-Host "  Please close KakaoTalk manually and run the script again." -ForegroundColor Yellow
            exit 1
        }
    } else {
        Write-Host "  ✓ KakaoTalk.exe is not running" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host ("=" * 50) -ForegroundColor Cyan
Write-Host "Step 2: Processing database files..." -ForegroundColor Yellow
Write-Host ("=" * 50) -ForegroundColor Cyan
Write-Host ""

# Define non-recoverable databases that should be deleted (no backup recovery possible)
$nonRecoverableDatabases = @(
    "emoticon.edb",      # Emoticon database - regenerated on login
    "talk_user_prf.edb"  # User preference database - regenerated on login
)

# Get all .edb files recursively (excluding those with .backup extension)
$edbFiles = Get-ChildItem -Path . -Filter "*.edb" -Recurse | Where-Object { $_.Extension -eq ".edb" }

if ($edbFiles.Count -eq 0) {
    Write-Host "No .edb files found in the current directory or subdirectories." -ForegroundColor Yellow
    exit
}

Write-Host "Found $($edbFiles.Count) .edb file(s) across all directories" -ForegroundColor Green
Write-Host ""

# Initialize counters for summary statistics
$totalProcessed = 0
$totalSkipped = 0
$totalDeleted = 0  # Special databases that are deleted (non-recoverable)
$totalRenamed = 0  # Renamed post-migration databases (preserving data)

# Group files by directory for better organization
$filesByDirectory = $edbFiles | Group-Object DirectoryName

foreach ($dirGroup in $filesByDirectory) {
    # Create relative path (compatible with older PowerShell versions)
    $currentPath = (Get-Location).Path
    $dirPath = $dirGroup.Name
    if ($dirPath.StartsWith($currentPath)) {
        $relativePath = $dirPath.Substring($currentPath.Length).TrimStart('\')
        if ([string]::IsNullOrEmpty($relativePath)) {
            $relativePath = "."
        }
    }
    else {
        $relativePath = $dirPath
    }
    
    Write-Host ""
    Write-Host "Directory: $relativePath" -ForegroundColor Cyan
    Write-Host ("-" * $DIRECTORY_SEPARATOR_LENGTH) -ForegroundColor Cyan
    
    foreach ($edbFile in $dirGroup.Group) {
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($edbFile.Name)
        $directory = $edbFile.DirectoryName
        
        # Check if this is a non-recoverable database that should be deleted
        if ($nonRecoverableDatabases -contains $edbFile.Name) {
            Write-Host "  Special case: $($edbFile.Name)" -ForegroundColor Magenta
            
            # Remove the database file safely
            if (Invoke-SafeOperation -Operation "delete special database" -Target $edbFile.Name -Action {
                Remove-Item -Path $edbFile.FullName -Force -ErrorAction Stop
            } -SuccessMessage "Deleted: $($edbFile.Name) (non-recoverable database)") {
                
                # Also remove associated WAL and SHM files if they exist
                $walFile = Join-Path $directory "$baseName.edb-wal"
                $shmFile = Join-Path $directory "$baseName.edb-shm"
                
                if (Test-Path $walFile) {
                    Invoke-SafeOperation -Operation "delete WAL file" -Target "$baseName.edb-wal" -Action {
                        Remove-Item -Path $walFile -Force -ErrorAction Stop
                    } -SuccessMessage "Deleted: $baseName.edb-wal" | Out-Null
                }
                
                if (Test-Path $shmFile) {
                    Invoke-SafeOperation -Operation "delete SHM file" -Target "$baseName.edb-shm" -Action {
                        Remove-Item -Path $shmFile -Force -ErrorAction Stop
                    } -SuccessMessage "Deleted: $baseName.edb-shm" | Out-Null
                }
                
                $totalDeleted++
            } else {
                $totalSkipped++
            }
            continue
        }
        
        # Find all backup files for this .edb file in the same directory
        # Pattern: originalname.edb_YYYYMMDD_HHMMSS.backup
        $backupPattern = "$baseName.edb_*.backup"
        $backupFiles = Get-ChildItem -Path $directory -Filter $backupPattern | Where-Object { 
            # Ensure it matches the exact pattern with timestamp
            $_.Name -match "^$([regex]::Escape($baseName))\.edb_\d{8}_\d{6}\.backup$"
        }
        
        if ($backupFiles.Count -gt 0) {
            Write-Host "  Processing: $($edbFile.Name)" -ForegroundColor Yellow
            Write-Host "    Found $($backupFiles.Count) backup file(s)" -ForegroundColor Gray
            
            # Use cached migration date (already parsed and validated)
            $migrationDate = $script:migrationDate
            
            # Find the first backup file AFTER migration timepoint (earliest backup after migration)
            $targetBackup = $null
            $earliestPostMigrationDate = [DateTime]::MaxValue
            $validBackups = @()
            
            foreach ($backup in $backupFiles) {
                # Extract timepoint from filename (format: originalname.edb_YYYYMMDD_HHMMSS.backup)
                if ($backup.Name -match "_(\d{8}_\d{6})\.backup$") {
                    $backupTimepoint = $matches[1]
                    
                    try {
                        # Parse backup timepoint
                        $backupDate = Parse-TimepointToDateTime $backupTimepoint "backup file"
                        
                        # If we have a migration date, only consider backups AT OR AFTER migration
                        if ($migrationDate -ne $null) {
                            if ($backupDate.Ticks -ge $migrationDate.Ticks) {
                                $validBackups += @{
                                    File = $backup
                                    Date = $backupDate
                                }
                                
                                # Find the earliest backup after migration (first failure backup)
                                if ($backupDate.Ticks -lt $earliestPostMigrationDate.Ticks) {
                                    $earliestPostMigrationDate = $backupDate
                                    $targetBackup = $backup
                                }
                            }
                        } else {
                            # No migration date available - this should not happen in normal flow
                            # Migration date should always be available from confirmed migration or manual input
                            Write-Host "    Warning: No migration date available for filtering backups" -ForegroundColor DarkYellow
                        }
                    }
                    catch {
                        Write-Host "      Warning: Could not parse date from $($backup.Name)" -ForegroundColor DarkYellow
                    }
                }
            }
            
            # If no valid backups found, cannot proceed safely
            if ($targetBackup -eq $null) {
                if ($migrationDate -ne $null) {
                    Write-Host "    ✗ No valid backups found after migration timepoint $selectedTimepoint" -ForegroundColor Red
                    Write-Host "    This could indicate:" -ForegroundColor Yellow
                    Write-Host "      1. All backups were created before migration (no post-migration backup exists)" -ForegroundColor Yellow
                    Write-Host "      2. Backup files have incorrect timestamp format" -ForegroundColor Yellow
                    Write-Host "      3. Migration timepoint is incorrect" -ForegroundColor Yellow
                } else {
                    Write-Host "    ✗ Cannot determine valid backup without migration timepoint" -ForegroundColor Red
                }
                $totalSkipped++
                continue
            }
            
            # Provide feedback about backup selection
            if ($migrationDate -ne $null -and $validBackups.Count -gt 0) {
                $afterMigrationCount = $validBackups.Count
                Write-Host "    Found $afterMigrationCount backup(s) after migration timepoint" -ForegroundColor Gray
            }
            
            # At this point, $targetBackup is guaranteed to be non-null
            $backupDescription = if ($migrationDate -ne $null) { "Target backup (first after migration)" } else { "Target backup" }
            Write-Host "    $backupDescription`: $($targetBackup.Name)" -ForegroundColor Gray
            
            # Prepare paths for atomic database restoration
            $newBackupPath = Join-Path $directory "$baseName.edb_$($script:CurrentTimestamp).backup.new"
            
            if ($script:WhatIfMode) {
                Write-Host "    [PREVIEW] Would restore: $($targetBackup.Name) → $baseName.edb" -ForegroundColor Cyan
                Write-Host "    [PREVIEW] Would archive: original → $baseName.edb_$($script:CurrentTimestamp).backup.new" -ForegroundColor Cyan
                
                # Check for WAL and SHM files to show in preview
                $walFile = Join-Path $directory "$baseName$WAL_EXTENSION"
                $shmFile = Join-Path $directory "$baseName$SHM_EXTENSION"
                $filesToRemove = @()
                if (Test-Path $walFile) { $filesToRemove += "$baseName$WAL_EXTENSION" }
                if (Test-Path $shmFile) { $filesToRemove += "$baseName$SHM_EXTENSION" }
                
                if ($filesToRemove.Count -gt 0) {
                    Write-Host "    [PREVIEW] Would remove: $($filesToRemove -join ', ')" -ForegroundColor Cyan
                }
                
                $totalProcessed++
            } else {
                $tempRestoreFile = Join-Path $directory "$baseName.edb.temp_restore"
                $tempOriginalFile = Join-Path $directory "$baseName.edb.temp_original"
                
                try {
                    # Step 1: Copy backup to temporary location first
                    Copy-Item -Path $targetBackup.FullName -Destination $tempRestoreFile -ErrorAction Stop
                    Write-Host "    ✓ Prepared restore file: $($targetBackup.Name)" -ForegroundColor Gray
                    
                    # Step 2: Move original to temp backup location (atomic on same filesystem)
                    Move-Item -Path $edbFile.FullName -Destination $tempOriginalFile -ErrorAction Stop
                    Write-Host "    ✓ Secured original: $($edbFile.Name)" -ForegroundColor Gray
                    
                    # Step 3: Move restored file to final location (atomic)
                    Move-Item -Path $tempRestoreFile -Destination $edbFile.FullName -ErrorAction Stop
                    Write-Host "    ✓ Restored: $($targetBackup.Name) → $baseName.edb" -ForegroundColor Green
                    
                    # Step 4: Move temp original to final backup name (atomic)
                    Move-Item -Path $tempOriginalFile -Destination $newBackupPath -ErrorAction Stop
                    Write-Host "    ✓ Archived: original → $baseName.edb_$($script:CurrentTimestamp).backup.new" -ForegroundColor Green
                    
                    # Step 5: Remove associated WAL and SHM files (incompatible with restored DB)
                    $walFile = Join-Path $directory "$baseName$WAL_EXTENSION"
                    $shmFile = Join-Path $directory "$baseName$SHM_EXTENSION"
                    
                    if (Test-Path $walFile) {
                        Remove-Item -Path $walFile -Force -ErrorAction SilentlyContinue
                        Write-Host "    ✓ Removed: $baseName$WAL_EXTENSION (incompatible with restored DB)" -ForegroundColor DarkGray
                    }
                    
                    if (Test-Path $shmFile) {
                        Remove-Item -Path $shmFile -Force -ErrorAction SilentlyContinue
                        Write-Host "    ✓ Removed: $baseName$SHM_EXTENSION (incompatible with restored DB)" -ForegroundColor DarkGray
                    }
                    
                    $totalProcessed++
                }
                catch {
                    # Atomic rollback - restore original state if any step failed
                    Write-Host "    ✗ Restoration failed: $_" -ForegroundColor Red
                    Write-Host "    Performing atomic rollback..." -ForegroundColor Yellow
                    
                    # Rollback in reverse order
                    if (Test-Path $tempOriginalFile) {
                        Move-Item -Path $tempOriginalFile -Destination $edbFile.FullName -ErrorAction SilentlyContinue
                        Write-Host "    ✓ Rollback: Original database restored" -ForegroundColor Yellow
                    }
                    if (Test-Path $tempRestoreFile) {
                        Remove-Item -Path $tempRestoreFile -Force -ErrorAction SilentlyContinue
                        Write-Host "    ✓ Rollback: Cleanup temporary files" -ForegroundColor Yellow
                    }
                    if (Test-Path $newBackupPath) {
                        Remove-Item -Path $newBackupPath -Force -ErrorAction SilentlyContinue
                    }
                    
                    $totalSkipped++
                }
            }
        }
        else {
            # No backup files found - check if database was created before or after migration
            try {
                $migrationDate = $script:migrationDate
                
                if ($edbFile.LastWriteTime.Ticks -gt $migrationDate.Ticks) {
                    # Database was modified after migration - rename it to preserve data
                    Write-Host "  Post-migration database: $($edbFile.Name)" -ForegroundColor Magenta
                    Write-Host "    Modified: $($edbFile.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss.fff'))" -ForegroundColor Gray
                    Write-Host "    Migration: $($migrationDate.ToString('yyyy-MM-dd HH:mm:ss.fff'))" -ForegroundColor Gray
                    
                    # Generate unique rename suffix to avoid conflicts
                    $renameSuffix = "_post_migration_$timestamp"
                    $newFileName = "$($edbFile.Name)$renameSuffix"
                    $newFilePath = Join-Path $directory $newFileName
                    
                    # Rename the post-migration database file safely to preserve data
                    if (Invoke-SafeOperation -Operation "rename post-migration database" -Target $edbFile.Name -Action {
                        Move-Item -Path $edbFile.FullName -Destination $newFilePath -Force -ErrorAction Stop
                    } -SuccessMessage "Renamed: $($edbFile.Name) → $newFileName (preserving post-migration data)") {
                        
                        # Also rename associated WAL and SHM files if they exist
                        $walFile = Join-Path $directory "$baseName.edb-wal"
                        $shmFile = Join-Path $directory "$baseName.edb-shm"
                        
                        if (Test-Path $walFile) {
                            $newWalName = "$baseName.edb-wal$renameSuffix"
                            $newWalPath = Join-Path $directory $newWalName
                            Invoke-SafeOperation -Operation "rename WAL file" -Target "$baseName.edb-wal" -Action {
                                Move-Item -Path $walFile -Destination $newWalPath -Force -ErrorAction Stop
                            } -SuccessMessage "Renamed: $baseName.edb-wal → $newWalName" | Out-Null
                        }
                        
                        if (Test-Path $shmFile) {
                            $newShmName = "$baseName.edb-shm$renameSuffix"
                            $newShmPath = Join-Path $directory $newShmName
                            Invoke-SafeOperation -Operation "rename SHM file" -Target "$baseName.edb-shm" -Action {
                                Move-Item -Path $shmFile -Destination $newShmPath -Force -ErrorAction Stop
                            } -SuccessMessage "Renamed: $baseName.edb-shm → $newShmName" | Out-Null
                        }
                        
                        $totalRenamed++
                    } else {
                        $totalSkipped++
                    }
                } else {
                    # Database was modified before migration and never opened after - keep it
                    Write-Host "  Pre-migration database: $($edbFile.Name)" -ForegroundColor Green
                    Write-Host "    Modified: $($edbFile.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss.fff'))" -ForegroundColor Gray
                    Write-Host "    Migration: $($migrationDate.ToString('yyyy-MM-dd HH:mm:ss.fff'))" -ForegroundColor Gray
                    Write-Host "    ✓ Keeping: Not modified after migration (legacy database)" -ForegroundColor Green
                    # No action needed - database is already in correct pre-migration state
                }
        }
        catch {
            Write-Host "  ✗ Error processing $($edbFile.Name): $($_.Exception.Message)" -ForegroundColor Red
            $totalSkipped++
        }
        }
    }
}

Write-Host ""
Write-Host ("=" * 50) -ForegroundColor Cyan

# Clean up keystore files after database processing
Write-Host ""
Write-Host "Cleaning up keystore and credential files..." -ForegroundColor Cyan
$keystoreFiles = Get-ChildItem -Path . -Recurse -Include "keystore.bin", "keystore.bin.backup", "credential.bin", "credential.bin.backup"
$keystoreDeleted = 0

foreach ($keystoreFile in $keystoreFiles) {
    $relativePath = $keystoreFile.FullName.Substring((Get-Location).Path.Length).TrimStart('\')
    if ([string]::IsNullOrEmpty($relativePath)) {
        $relativePath = $keystoreFile.Name
    }
    
    try {
        Remove-Item -Path $keystoreFile.FullName -Force -ErrorAction Stop
        Write-Host "  ✓ Deleted: $relativePath (invalid after DB revert)" -ForegroundColor DarkYellow
        $keystoreDeleted++
    }
    catch {
        Write-Host "  ✗ Failed to delete: $relativePath - $_" -ForegroundColor Red
    }
}

if ($keystoreDeleted -eq 0) {
    Write-Host "  No keystore files found" -ForegroundColor Gray
}

Write-Host ""
Write-Host ("=" * 50) -ForegroundColor Cyan
Write-Host "Process completed!" -ForegroundColor Cyan
Write-Host ""

Write-Host "Summary:" -ForegroundColor Yellow
Write-Host "  Directories scanned: $($filesByDirectory.Count)" -ForegroundColor Gray
Write-Host "  Total .edb files found: $($edbFiles.Count)" -ForegroundColor Gray
Write-Host "  Files processed (restored): $totalProcessed" -ForegroundColor Green
Write-Host "  Files deleted (special cases): $totalDeleted" -ForegroundColor DarkYellow
Write-Host "  Files renamed (post-migration, preserved): $totalRenamed" -ForegroundColor Yellow
Write-Host "  Keystore files deleted: $keystoreDeleted" -ForegroundColor DarkYellow
Write-Host "  Files skipped/errors: $totalSkipped" -ForegroundColor DarkGray

# Resource cleanup and final validation
Write-Host ""
if ($totalSkipped -gt 0) {
    Write-Host "⚠️  WARNING: $totalSkipped files had errors and were not processed." -ForegroundColor Yellow
    Write-Host "   Review the output above for specific error details." -ForegroundColor Yellow
}

# Clean up script-level variables to free memory
if ($script:backupFileCache) {
    $cacheSize = $script:backupFileCache.Keys.Count
    $script:backupFileCache = $null
    Write-Host "Cleaned up backup file cache ($cacheSize entries)" -ForegroundColor DarkGray
}
$script:migrationDate = $null

# Final status
$totalOperations = $totalProcessed + $totalDeleted + $totalRenamed + $keystoreDeleted
if ($totalOperations -gt 0 -and $totalSkipped -eq 0) {
    Write-Host "✅ All operations completed successfully!" -ForegroundColor Green
} elseif ($totalOperations -gt 0) {
    Write-Host "⚠️  Operations completed with some warnings/errors." -ForegroundColor Yellow
} else {
    Write-Host "ℹ️  No operations were needed." -ForegroundColor Cyan
}