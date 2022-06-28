package main

import (
	generatedfiles "Go-Grpc/generatedfiles"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"

	_ "github.com/lib/pq"

	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 8056, "The server port")
)

type Students struct {
	Name string
	Id   int
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

func (s AssignTeamMateService) AssignTeamMate(ctx context.Context, request *generatedfiles.AssignTeamRequestgRPC) (*generatedfiles.AssignTeamResponsegRPC, error) {
	//start paste

	if request.GetId() == 0 || request.GetCourseCode() == "" {
		return &generatedfiles.AssignTeamResponsegRPC{Message: "You are missing required paramters parameter."}, nil
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
			if students[i].Id == int(request.GetId()) {
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
			//teamDetails := TeamDetails{Member1: request.GetName(), Member2: assignedTeamMate.Name, Member1id: int(request.GetId()), Member2id: assignedTeamMate.Id, CourseCode: request.GetCourseCode()}
			var lastInsertID int = 0
			err := db.QueryRow("INSERT INTO team_info(member1,member2,member1_id,member2_id,course_code) VALUES($1, $2, $3, $4, $5) RETURNING group_id;", request.GetName(), assignedTeamMate.Name, request.GetId(), assignedTeamMate.Id, request.GetCourseCode()).Scan(&lastInsertID)
			db.Close()
			if err != nil {
				panic(err)
			}
			return &generatedfiles.AssignTeamResponsegRPC{Message: "The team has been inserted successfully!", TeamDetails: &generatedfiles.TeamDetailsgRPC{Member1: request.GetName(), Member2: assignedTeamMate.Name, CourseCode: request.GetCourseCode()}}, nil
		} else {
			return &generatedfiles.AssignTeamResponsegRPC{Message: "Requesting Student's details are missing in db!"}, nil
		}

	}
	//end paste
	return nil, nil
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
