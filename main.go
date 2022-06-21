package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"

	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 8056, "The server port")
)

type Students struct {
	Name string
	Id   int
}
type TeamDetails struct {
	Member1    string
	Member2    string
	Member1id  int
	Member2id  int
	CourseCode string
	GroupId    int
}

const (
	DB_USER     = "postgres"
	DB_PASSWORD = "root123"
	DB_NAME     = "dissertation"
)

func setupDB() *sql.DB {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", DB_USER, DB_PASSWORD, DB_NAME)
	DB, err := sql.Open("postgres", dbinfo)

	if err != nil {
		panic(err)
	}

	return DB
}

// server is used to implement helloworld.GreeterServer.
type AssignTeamMateService struct {
	generatedfiles.UnimplementedAssignTeamMateServer
	//generatedfiles.UnimplementedAssignTeamMateServer
}

func (s AssignTeamMateService) AssignTeamMate(context.Context, *generatedfiles.AssignTeamRequestgRPC) (*generatedfiles.AssignTeamResponsegRPC, error) {
	//start paste

	if &generatedfiles.AssignTeamRequestgRPC.Id == 0 || &generatedfiles.AssignTeamRequestgRPC.CourseCode == "" {
		&generatedfiles.AssignTeamResponsegRPC{Message: "You are missing required paramters parameter."}
	} else {
		db := setupDB()
		rows, err := db.Query("SELECT * FROM students")
		if err != nil {
			panic(err)
		}
		//fmt.Println("Hello db has been setup!")
		var students []Students
		for rows.Next() {
			var id int
			var name string

			err = rows.Scan(&name, &id)

			if err != nil {
				panic(err)
			}

			students = append(students, Students{Name: name, Id: id})
		}
		var flag int = 0
		//fmt.Println("Hello students have been retrieved!", students)
		//fmt.Println("Size of students array", len(students))
		var filteredStudents []Students
		for i := 0; i < len(students); i++ { //looping from 0 to the length of the array
			if students[i].Id == &generatedfiles.AssignTeamRequestgRPC.Id {
				flag = 1
			} else {
				filteredStudents = append(filteredStudents, students[i])
			}
		}
		//fmt.Println("Size of filtered students array", len(filteredStudents))
		//fmt.Println(filteredStudents)
		if flag == 1 {
			n := rand.Int() % len(filteredStudents)
			assignedTeamMate := Students{Name: filteredStudents[n].Name, Id: filteredStudents[n].Id}
			teamDetails := TeamDetails{Member1: &generatedfiles.AssignTeamRequestgRPC.Name, Member2: assignedTeamMate.Name, Member1id: &generatedfiles.AssignTeamRequestgRPC.Id, Member2id: assignedTeamMate.Id, CourseCode: &generatedfiles.AssignTeamRequestgRPC.CourseCode}
			var lastInsertID int = 0
			err := db.QueryRow("INSERT INTO team_info(member1,member2,member1_id,member2_id,course_code) VALUES($1, $2, $3, $4, $5) RETURNING group_id;", &generatedfiles.AssignTeamRequestgRPC.Name, assignedTeamMate.Name, &generatedfiles.AssignTeamRequestgRPC.Id, assignedTeamMate.Id, &generatedfiles.AssignTeamRequestgRPC.CourseCode).Scan(&lastInsertID)
			db.Close()
			if err != nil {
				panic(err)
			}
			&generatedfiles.AssignTeamResponsegRPC{Message: "The team has been inserted successfully!", TeamDetails: teamDetails}
		} else {
			&generatedfiles.AssignTeamResponsegRPC{Message: "Requesting Student's details are missing in db!"}
		}

	}
	//end paste
	return &generatedfiles.AssignTeamResponsegRPC{}
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	generatedfiles.RegisterAssignTeamMateServer(s, &AssignTeamMateService{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
